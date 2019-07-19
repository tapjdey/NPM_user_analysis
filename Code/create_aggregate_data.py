import gzip    
import multiprocessing as mp
from collections import defaultdict
import json

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

def load_file(f, sep, single = 0):
    return_dict = defaultdict(set)
    for line in f:
        items = line.strip().split(sep)
        if len(items) <2 : continue
        if single: 
            return_dict[items[0]].add(tuple(items[1:]))
        else:
            for el in items[1:]:
                return_dict[items[0]].add(el)
    return return_dict

def mp_run_f(login):
    p= oneAuthor(login) 
    if p.write_f :
        print(json.dumps(p.__dict__,  cls=SetEncoder))
    




if __name__ == '__main__':
    with open('/da4_data/play/Tapajit/NPM_P2m_OCT18_Lerna/P2m_lerna','r') as f:
        lerna_dep = load_file(f, ';')  
    with open('/home/tdey2/work/npms/issues/issues_10k_author_mod_author.csv', 'r') as f:
        issue_author = {}
        for line in f:
            items = line.strip().split(',')
            if items[0] not in issue_author.keys() :
                issue_author[items[0]] = \
                {items[1] : {'count' : 1, 'pull_req' : int(items[3]), 'pkgs': set(items[2].split(';'))}}
            elif items[1] not in issue_author[items[0]].keys():
                issue_author[items[0]][items[1]] = \
                {'count' : 1, 'pull_req' : int(items[3]),  'pkgs': set(items[2].split(';'))}
            else:
                issue_author[items[0]][items[1]]['count'] += 1
                issue_author[items[0]][items[1]]['pull_req'] += int(items[3])
                issue_author[items[0]][items[1]]['pkgs'].update(set(items[2].split(';')))
    with open('/da4_data/play/Tapajit/login_url_mod.csv', 'rt') as f:
        login_url = load_file(f, ',')
    with open('/home/tdey2/work/npms/issues/repo_pkgname_mod.csv','r') as f:
        repo_pkg_map = load_file(f, ';')
    with open('/home/tdey2/work/npms/issues/repo_pkgname_reverse.csv','r') as f:
        repo_pkg_map_rev = load_file(f, ',')
    with open('/da4_data/play/Tapajit/Tapajit_edgelist_recursive.csv', 'r', encoding="ISO-8859-1") as f:
        rec_deplist = load_file(f, ',', 1)
    with open('/da4_data/play/Tapajit/allJS', 'r', encoding="ISO-8859-1") as f:
        prj_dep = load_file(f, ';',1)
    with open('/da4_data/play/Tapajit/fork_mod2.csv','r') as f:
        fork_repo = load_file(f, ',')
    with open('/home/tdey2/work/npms/user/users.csv', 'r', encoding="ISO-8859-1") as f:
        users = {}
        f.readline()
        for line in f:
            items = line.strip().split(',')
            if items[1] == "None": com = 0
            else: com = 1
            users[items[0]] = {'Company': com, 'Bot': int(items[-1])}
    class oneAuthor:
        def __init__(self, login):
            self.write_f = 1
            self.login = login
            self.company = users[login]['Company']
            self.bot = users[login]['Bot']
            
            self.dep = defaultdict(int)
            
            # projects commited to
            try:
                prjlist = login_url[login]
            except:
                prjlist = []
            
            forked_prjlist = set()
            depend = {}
            for p in prjlist:
                # get fork
                if p in fork_repo: 
                    forked_prjlist.update(fork_repo[p])
                # get direct pkg list
                if p in repo_pkg_map:
                    for m in repo_pkg_map[p]:
                        self.dep[m] = 0
                # get dependencies
                if p in prj_dep:
                    depend[p] = set(';'.join(list(prj_dep[p])[0]).split(';'))
                if p in lerna_dep:
                    if p in depend.keys():
                        depend[p] = depend[p] | lerna_dep[p]
                    else:
                        depend[p] = lerna_dep[p]
            try:
                alldeps = set.union(*list(depend.values()))
            except:
                alldeps = []
            for v in alldeps:
                if v not in self.dep: self.dep[v] = 1
            depend = {}
            # check if a fork is package and add dependencies
            for p in forked_prjlist:
                if p in repo_pkg_map:
                    for m in repo_pkg_map[p]:
                        self.dep[m] = 1
                if p in prj_dep:
                    depend[p] = set(';'.join(list(prj_dep[p])[0]).split(';'))
                if p in lerna_dep:
                    if p in depend.keys():
                        depend[p] = depend[p] | lerna_dep[p]
                    else:
                        depend[p] = lerna_dep[p]
            try:
                alldeps_f = set.union(*list(depend.values()))
            except:
                alldeps_f =[]
            for v in alldeps_f:
                if v not in self.dep: 
                    self.dep[v] = 2
            # get layered dependency
            try: 
                full_deps = set(self.dep.keys())
            except:
                full_deps = []
            seen = full_deps.copy()
            for p in full_deps:
                if p in rec_deplist:
                    rd = rec_deplist[p]
                    for v in rd:
                        if v[0] not in seen:
                            seen.add(v[0])
                            if p in self.dep:
                                self.dep[v[0]] = int(v[1]) + self.dep[p] 
                            else:
                                self.dep[v[0]] = int(v[1])
                                
            # Issues
            try:
                self.issues = issue_author[login]
                self.n_issues = sum([x['count'] for x in self.issues.values()])
                self.n_pull_req = sum([x['pull_req'] for x in self.issues.values()])
            except:
                self.write_f = 0
                return        
            # Issues by dependency layer
            self.issue_dep_layer = {}
            seen = set()
            try:
              for v in range(max(self.dep.values()) + 1):
                self.issue_dep_layer[v] = {'count' : 0, 'pull_req' : 0}
                for i in range(len(self.dep.values())):
                    if list(self.dep.values())[i] == v:                        
                        try: pkgurl = list(repo_pkg_map_rev[list(self.dep.keys())[i]])[0]
                        except: continue
                        if pkgurl not in seen :                             
                            seen.update([pkgurl])                            
                            if pkgurl in self.issues.keys():
                                self.issue_dep_layer[v]['count'] += self.issues[pkgurl]['count']
                                self.issue_dep_layer[v]['pull_req'] += self.issues[pkgurl]['pull_req']
            
              self.issue_dep_layer['none'] = {'count' : 0, 'pull_req' : 0}
              for p in (set(self.issues.keys()) -seen):
                if p in self.issues.keys():
                    self.issue_dep_layer['none']['count'] += self.issues[p]['count']
                    self.issue_dep_layer['none']['pull_req'] += self.issues[p]['pull_req']
            except:
                pass
                        
            
    pool = mp.Pool(mp.cpu_count())
    result = pool.map(mp_run_f, list(users.keys()))
        

#!/usr/bin/env python3
"""Remove local_includes already supplied by a library's dependencies.

Only a consumer's own implementation_deps or transitive public deps can
supply an include directory. Each shared list must be covered in every use;
all other expanded attributes retain their source spelling and values.
"""
import argparse
import ast
from collections import Counter, defaultdict
from functools import cache
import hashlib
import shutil
import json
from pathlib import Path
import posixpath
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from initial_migration.repository_inputs import ROOT
from initial_migration.compact_build import Source, Expand, ast_key
from initial_migration.first_party_packages import canonical_dependency
from initial_migration.native_public_includes import directory, list_elements
from initial_migration.prune_inherited_headers import CC_COMPILERS, CC_PROVIDERS, Document, Graph

CPUS = ('x86_64','aarch64','default')
LLVM_DIRS = ('contrib/llvm-project/', 'external/llvm')

def canonical_dir(node, doc):
    value = directory(node, doc)
    if value is None or value.startswith('$(GENDIR)') or '$(' in value:
        return None
    if isinstance(node, ast.Call) and getattr(node.func,'id','') == 'repository_local_include':
        actual = posixpath.normpath(value)
    else:
        actual = posixpath.normpath(posixpath.join(doc.package or doc.owner or '',value))
    # Toolchain and ABI headers are managed by the toolchain and macro; a
    # partial repository graph is not evidence of visibility for those dirs.
    if actual == 'contrib/llvm-project' or actual.startswith(LLVM_DIRS + ('contrib/sysroot',)):
        return None
    return actual

def file_hash(text):
    return hashlib.sha256(text.encode()).hexdigest()

class PublicIncludes(Graph):
    @cache
    def own_public_dirs(self,target,cpu):
        if target not in self.rules:
            return {}
        doc,kind,attrs=self.rules[target]
        if kind not in CC_PROVIDERS:
            return {}
        result = {}
        for item in list_elements(attrs.get('includes'),doc,cpu):
            value = canonical_dir(item, doc)
            if value is not None:
                result[value] = target
        return result

    @cache
    def available(self, target, cpu):
        if target not in self.rules:
            return {}
        doc,kind,attrs = self.rules[target]
        pending = list(self.dependencies(target,cpu))
        if kind in CC_COMPILERS:
            pending += list(doc.labels(attrs.get('implementation_deps'),cpu))
        # Only public deps propagate after the consumer's direct private deps.
        visited = {target}
        result={}
        while pending:
            ref=pending.pop()
            if ref in visited or ref not in self.rules:
                continue
            visited.add(ref)
            other,typ,fields=self.rules[ref]
            if typ not in CC_PROVIDERS | {'alias'}:
                continue
            result.update(self.own_public_dirs(ref,cpu))
            pending.extend(self.dependencies(ref,cpu))
        return result

class Rewrite:
    def __init__(self,doc,graph):
        self.doc = doc
        self.graph = graph
        self.lists = {}
        self.records = {}
        self.visited = set()

    def walk(self,node,target,cpus,allowed,constants=(),collect=True):
        if node is None:
            return
        if isinstance(node,ast.Name) and node.id in self.doc.constants and node.id not in constants:
            key=(node.id, target, cpus, allowed, collect)
            if key not in self.visited:
                self.visited.add(key)
                self.walk(self.doc.constants[node.id],target,cpus,allowed,(*constants,node.id),collect)
            return
        if isinstance(node,(ast.List,ast.Tuple)):
            for item in node.elts:
                value = canonical_dir(item,self.doc)
                if value is None:
                    self.walk(item, target, cpus, allowed, constants, collect)
                    continue
                if collect:
                    record=self.records.setdefault(id(item),{
                        'node':item,'container':node,'value':value,'valid':True,'witness':[],
                    })
                    self.lists[id(node)]=node
                elif id(item) in self.records:
                    record=self.records[id(item)]
                else:
                    continue
                if not allowed or not cpus or target is None:
                    record['valid']=False
                else:
                    for cpu in cpus:
                        ref=self.graph.available(target,cpu).get(value)
                        if ref is None:
                            record['valid']=False
                        else:
                            record['witness'].append((target,cpu,ref))
            return
        if isinstance(node,ast.BinOp) and isinstance(node.op,ast.Add):
            self.walk(node.left,target,cpus,allowed,constants,collect)
            self.walk(node.right,target,cpus,allowed,constants,collect)
            return
        if isinstance(node,ast.Call) and getattr(node.func,'id','') == 'select' and len(node.args)==1 and isinstance(node.args[0],ast.Dict):
            table=node.args[0]
            keys={key.value for key in table.keys if isinstance(key,ast.Constant)}
            known=len(keys)==len(table.keys) and keys <= {
                '@platforms//cpu:x86_64','@platforms//cpu:aarch64','//conditions:default'}
            for key,branch in zip(table.keys,table.values):
                match=cpus
                if known:
                    match=tuple(cpu for cpu in cpus if (
                        key.value=='@platforms//cpu:'+cpu or key.value=='//conditions:default' and '@platforms//cpu:'+cpu not in keys))
                self.walk(branch,target,match,allowed,constants,collect)
            return
        for child in ast.iter_child_nodes(node):
            self.walk(child,target,cpus,False,constants,collect)

    def text(self):
        source=self.doc.source
        calls=list(source.rules())
        for _,rule in calls:
            attrs={kw.arg:kw.value for kw in rule.keywords}
            name=attrs.get('name')
            kind=getattr(rule.func,'id','')
            if kind not in CC_COMPILERS or not isinstance(name,ast.Constant) or not isinstance(name.value,str):
                continue
            self.walk(attrs.get('local_includes'),self.doc.reference(':'+name.value),CPUS,True)
        # A constant also used in a different attribute or a non-compiler rule
        # cannot be rewritten through this pass.
        for stmt in source.module.body:
            if isinstance(stmt,ast.Assign):
                continue
            if isinstance(stmt,ast.Expr) and isinstance(stmt.value,ast.Call):
                call=stmt.value
                for kw in call.keywords:
                    if getattr(call.func,'id','') in CC_COMPILERS and kw.arg=='local_includes':
                        continue
                    self.walk(kw.value,None,CPUS,False,collect=False)
                for arg in call.args:
                    self.walk(arg,None,CPUS,False,collect=False)
            else:
                self.walk(stmt,None,CPUS,False,collect=False)
        edits=[]
        final=[]
        for key,node in self.lists.items():
            retained=[]
            changed=False
            for item in node.elts:
                record=self.records.get(id(item))
                if record is not None and record['valid'] and record['witness']:
                    changed=True
                    final.append({
                        'directory':record['value'],
                        'witness':[list(v) for v in sorted(set(record['witness']))],
                        'original':source.text(item),
                    })
                else:
                    retained.append(source.text(item))
            if changed:
                edits.append((*source.span(node),'[\n' + ''.join('    '+x+',\n' for x in retained)+']'))
        updated=source.rewrite(edits)
        if edits:
            updated=remove_empty_local_lists(updated)
        # Expanded attrs comparison also checks shared constants did not change
        # any source file, dep, compiler option, data, or exported rule label.
        prove_unchanged(source.bytes.decode(),updated,self.doc.path)
        return updated,final

def remove_empty_local_lists(text):
    doc=Source(text)
    constants={s.targets[0].id:s.value for s in doc.module.body if isinstance(s,ast.Assign)
               and len(s.targets)==1 and isinstance(s.targets[0],ast.Name)}
    def pure_empty(node,seen=()):
        if isinstance(node,(ast.List,ast.Tuple)):
            return not node.elts
        if isinstance(node,ast.Name) and node.id in constants and node.id not in seen:
            return pure_empty(constants[node.id],(*seen,node.id))
        if isinstance(node,ast.BinOp) and isinstance(node.op,ast.Add):
            return pure_empty(node.left,seen) and pure_empty(node.right,seen)
        # Never erase a select: even []-valued arms carry rejection semantics.
        return False
    edits=[]
    for _,call in doc.rules():
        if getattr(call.func,'id','') not in CC_COMPILERS:
            continue
        for kw in call.keywords:
            if kw.arg=='local_includes' and pure_empty(kw.value):
                start,end=doc.span(kw)
                if doc.bytes[end:end+1] == b',':
                    end+=1
                edits.append((start,end,''))
    return doc.rewrite(edits)

def expanded_rules(text):
    tree=Expand().visit(ast.parse(text))
    result={}
    for stmt in tree.body:
        if not isinstance(stmt,ast.Expr) or not isinstance(stmt.value,ast.Call):
            continue
        call=stmt.value
        attrs={kw.arg:kw.value for kw in call.keywords}
        name=attrs.get('name')
        if not isinstance(name,ast.Constant) or not isinstance(name.value,str):
            continue
        if name.value in result:
            raise ValueError('duplicate target '+name.value)
        result[name.value]=(getattr(call.func,'id',''),attrs)
    return result

def prove_unchanged(before,after,path):
    old,new=expanded_rules(before),expanded_rules(after)
    if set(old)!=set(new):
        raise ValueError('changed target names '+str(path))
    for name,(kind,fields) in old.items():
        nk, nf=new[name]
        skip={'local_includes'} if kind in CC_COMPILERS else set()
        if kind!=nk or (set(fields)-skip)!=(set(nf)-skip):
            raise ValueError(f'changed attr names {path} {name}')
        for field in set(fields)-skip:
            if ast_key(fields[field])!=ast_key(nf[field]):
                raise ValueError(f'changed protected attr {path} {name} {field}')

def run(root, *, write=False):
    paths=subprocess.check_output(
        ['git','ls-files','-z','--cached','--others','--exclude-standard','--','*.bazel'],
        cwd=root).decode().split('\0')
    docs=[]
    for p in dict.fromkeys(paths):
        path=root/p
        if not p or not path.is_file() or (path.name!='BUILD.bazel' and not path.name.endswith('.BUILD.bazel')):
            continue
        package=path.parent.relative_to(root).as_posix().removeprefix('.') if path.name=='BUILD.bazel' else ''
        docs.append(Document(path,package=package))
    graph=PublicIncludes(docs)
    originals={}
    outputs={}
    report={}
    counts=Counter()
    by_directory=Counter()
    by_repo=Counter()
    protected=Counter()
    for doc in docs:
        path=doc.path
        original=doc.source.bytes.decode()
        rewrite=Rewrite(doc,graph)
        after,removals=rewrite.text()
        protected['not_published_in_all_consumers'] += sum(
            not r['valid'] or not r['witness'] for r in rewrite.records.values())
        if not removals:
            continue
        proc=subprocess.run([shutil.which('buildifier'), '-type=build', '-mode=fix'],
            input=after, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        after=proc.stdout
        prove_unchanged(original,after,path)
        originals[path]=original
        outputs[path]=after
        report[path.relative_to(root).as_posix()]=removals
        counts['files']+=1
        counts['directories_removed']+=len(removals)
        counts['targets']+=len({t for d in removals for t,_,_ in d['witness']})
        for d in removals:
            by_directory[d['directory']]+=1
            by_repo[doc.repo]+=1
    # Recheck against the entire final graph; no witness may vanish as a
    # consequence of another target's cleanup.
    docs2=[Document(d.path,package=d.package,text=outputs.get(d.path,d.source.bytes.decode())) for d in docs]
    graph2=PublicIncludes(docs2)
    for d in docs2:
        after,more=Rewrite(d,graph2).text()
        if more or after != d.source.bytes.decode():
            raise ValueError('include cleanup is not idempotent: '+str(d.path))
    if write:
        for path, original in originals.items():
            if path.read_text()!=original:
                raise ValueError('BUILD changed during include proof: '+str(path))
        for path,after in outputs.items():
            path.write_text(after)
    return {
        'counts': dict(counts),
        'provider_directories': dict(by_directory.most_common()),
        'repositories': dict(by_repo.most_common()),
        'protected': dict(protected),
        'checks': {'unrelated_expanded_attrs':True,'buildifier':True,'idempotent':True},
        'before_sha256':{str(p.relative_to(root)):file_hash(s) for p,s in originals.items()},
        'after_sha256':{str(p.relative_to(root)):file_hash(s) for p,s in outputs.items()},
        'files': report,
    }


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=ROOT)
    parser.add_argument('--report',type=Path)
    parser.add_argument('--write',action='store_true')
    args=parser.parse_args()
    result=run(args.root.resolve(),write=args.write)
    if args.report:
        args.report.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({key:value for key,value in result.items()
                      if key not in {'files','before_sha256','after_sha256'}},indent=2))

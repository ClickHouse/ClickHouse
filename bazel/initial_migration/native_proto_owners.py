#!/usr/bin/env python3
"""Use the actual .pb.cc compiler as the source of its generated public hdrs.

--write applies only when both platform graphs, sources, and deps proofs pass.
--output optionally records the prospective BUILD files and proof receipt.
The generator calls transfer() after initial native packages exist.
"""
import argparse
import ast
from collections import Counter, defaultdict, deque
from functools import cache
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from initial_migration.repository_inputs import ROOT
from initial_migration.prune_inherited_headers import (
    ARCHES, CC_COMPILERS, CC_PROVIDERS, HEADER_FIELDS, Document, PublishedGraph, PublishedRewrite,
)
from initial_migration.compact_build import ast_key
from initial_migration.first_party_packages import Build, git_inventory
from initial_migration.native_textual_inputs import expanded

GENERATED = '@clickhouse//bazel:gen/'
PB_FILES = ('.pb.h', '.pb.cc')
PROTECTED_KINDS = ('srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs', 'deps')
OTHER_CPU = 'unknown_unsupported_cpu'
PREFERRED = {'@clickhouse_libhdfs3//:_hdfs3__compile_0': '@clickhouse_libhdfs3//:_hdfs3'}


def source_stem(ref):
    return ref.removesuffix('.h').removesuffix('.cc').removesuffix('.pb').removesuffix('.grpc')


def snapshot(root):
    names = subprocess.check_output([
        'git','ls-files','-z','--cached','--others','--exclude-standard',
        '--','BUILD.bazel','**/BUILD.bazel',
    ], cwd=root).decode().split('\0')
    paths = sorted({root / name for name in names if name and (root / name).is_file()})
    packages = {p.parent.relative_to(root).as_posix().removeprefix('.') for p in paths}
    files = git_inventory(root)
    result = []
    for path in paths:
        build = Build(root, path, files, packages)
        result.append(Document(path, package=build.package, files=build.inventory))
    result.extend(Document(path) for path in sorted((root/'bazel/dependencies').glob('*.BUILD.bazel')))
    return result


class BorrowingGraph(PublishedGraph):
    def __init__(self, root, docs):
        super().__init__(root, docs)
        self.planned = defaultdict(set)

    @cache
    def dependencies(self, target, cpu):
        return (*super().dependencies(target, cpu), *sorted(self.planned.get((target,cpu),())))

    def flush(self):
        self.dependencies.cache_clear()
        self.inherited.cache_clear()
        self.reaches.cache_clear()

    @cache
    def reaches(self, first, last, cpu):
        seen, q, trace = {first}, deque([first]), {first:None}
        while q:
            node = q.popleft()
            if node == last:
                found = [node]
                while trace[found[-1]] is not None:
                    found.append(trace[found[-1]])
                return tuple(reversed(found))
            pending = list(self.dependencies(node,cpu))
            if node in self.rules:
                doc, kind, attrs = self.rules[node]
                if kind in CC_COMPILERS:
                    pending.extend(doc.labels(attrs.get('implementation_deps'),cpu))
            for ref in pending:
                if ref not in seen:
                    seen.add(ref); trace[ref] = node; q.append(ref)
        return ()

    def public(self, target, cpu):
        return self.own_headers(target,cpu) | self.inherited(target,cpu)


def typed_options(field,ref):
    kinds = [field] if field in {'hdrs','textual_hdrs'} else ['hdrs', 'textual_hdrs']
    return frozenset((kind,ref) for kind in kinds)


def compile_only(field):
    return field in {'srcs', 'additional_compiler_inputs'}


def generated_proto(ref, cpu, graph):
    return (ref.startswith(GENERATED) and ref.endswith(PB_FILES)
            and graph.physical_file(ref, cpu))


def find_owners(graph):
    owners = defaultdict(set)
    compilers = defaultdict(set)
    for target,(doc,kind,attrs) in graph.rules.items():
        if kind not in CC_COMPILERS:
            continue
        for cpu in ARCHES:
            for ref in doc.labels(attrs.get('srcs'),cpu):
                if generated_proto(ref,cpu,graph) and ref.endswith('.pb.cc'):
                    compilers[ref,cpu].add(target)
                    for typ,hdr in graph.own_headers(target,cpu):
                        if generated_proto(hdr,cpu,graph) and source_stem(ref)==source_stem(hdr):
                            owners[typ,hdr,cpu].add(target)
    return owners,compilers


def plan_transfers(graph):
    owners, compiled = find_owners(graph)
    candidates = defaultdict(set)
    planned = defaultdict(set)
    decisions = Counter()
    witnesses = {}
    for target,(doc,kind,attrs) in graph.rules.items():
        if kind not in CC_COMPILERS or doc.repo != '@clickhouse':
            continue
        if 'include_prefix' in attrs or 'strip_include_prefix' in attrs:
            continue
        for cpu in ARCHES:
            for field in HEADER_FIELDS:
                for ref in doc.labels(attrs.get(field),cpu):
                    if not generated_proto(ref,cpu,graph):
                        continue
                    types = typed_options(field,ref)
                    typed_owners = set().union(*(owners[typ,hdr,cpu] for typ,hdr in types))
                    if target in typed_owners or (field=='srcs' and target in compiled[ref,cpu]):
                        continue
                    if not typed_owners:
                        decisions['unresolved_not_published_by_proto_compiler'] += 1
                        continue
                    original = graph.inherited(target,cpu,compile_only(field))
                    candidates[target,cpu,field].add(ref)
                    if original & types:
                        decisions['already_inherited'] += 1
                        witnesses[target,cpu,field,ref] = sorted(typed_owners)
                        continue
                    acyclic = []
                    for owner in sorted(typed_owners):
                        if graph.reaches(owner,target,cpu):
                            continue
                        chosen = PREFERRED.get(owner,owner)
                        if chosen != owner:
                            if not (graph.public(chosen,cpu) & types):
                                raise ValueError(f'{chosen} does not forward {owner} public {types}')
                            if graph.reaches(chosen,target,cpu):
                                raise ValueError(f'{chosen} wrapper creates cycle to {target}')
                        acyclic.append(chosen)
                    if not acyclic:
                        candidates[target,cpu,field].remove(ref)
                        decisions['would_cycle'] += 1
                        continue
                    if len(acyclic) != 1:
                        raise ValueError(f'{target} {cpu} {field} {ref}: multiple compilers {acyclic}')
                    provider, = acyclic
                    witnesses[target,cpu,field,ref] = [provider]
                    if provider not in graph.dependencies(target,cpu):
                        planned[target,cpu].add(provider)
                        decisions['missing_proto_owner'] += 1
    graph.planned = planned
    graph.flush()
    # This is a simultaneous graph verification: adding different edges in
    # different native consumers must not combine into a new dependency cycle.
    for (target,cpu),deps in planned.items():
        for dep in deps:
            trail = graph.reaches(dep,target,cpu)
            if trail:
                raise ValueError(f'new proto dependency {target} -> ' + ' -> '.join(trail))
    for (target,cpu,field),refs in candidates.items():
        actual = graph.inherited(target,cpu,compile_only(field))
        for ref in refs:
            if not (actual & typed_options(field,ref)):
                raise ValueError(f'planned dependency does not supply {target} {cpu} {field} {ref}')
    return candidates, planned, decisions, witnesses, compiled


class ProtoRewrite(PublishedRewrite):
    def __init__(self, doc, graph, candidates):
        super().__init__(doc,graph)
        self.candidates = candidates

    def covered(self,value,target,arches,field):
        ref=self.doc.reference(value)
        if not arches or not all(
                ref in self.candidates.get((target,cpu,field),()) for cpu in arches):
            return False
        if field == 'additional_compiler_inputs' and ref.endswith('.pb.cc'):
            return all(
                generated_proto(ref,cpu,self.graph)
                and ('textual_hdrs',ref) in self.graph.inherited(target,cpu,True)
                for cpu in arches)
        return super().covered(value,target,arches,field)


def dependency_spelling(ref):
    return ref.removeprefix('@clickhouse') if ref.startswith('@clickhouse//') else ref


def deps_node(additions, names):
    rows=[]
    for cpu in ARCHES:
        values=tuple(sorted(additions[cpu]))
        literal=names.get(values) or json.dumps([dependency_spelling(r) for r in values],indent=4)
        rows.append(json.dumps('@platforms//cpu:'+cpu)+': '+literal)
    # Never add a dependency or error for a CPU not supported by this batch.
    rows.append('"//conditions:default": []')
    return 'select({'+',\n'.join(rows)+'})'


def append_deps(doc, planned):
    fields={}
    references=Counter(n.id for n in ast.walk(doc.source.module)
                       if isinstance(n,ast.Name) and isinstance(n.ctx,ast.Load))
    for _,rule in doc.source.rules():
        attrs={k.arg:k.value for k in rule.keywords}
        if getattr(rule.func,'id','') not in CC_COMPILERS or not isinstance(attrs.get('name'),ast.Constant):
            continue
        target=doc.reference(':'+attrs['name'].value)
        additions={cpu:planned.get((target,cpu),set()) for cpu in ARCHES}
        if any(additions.values()):
            fields[target]=(rule,attrs,additions)
    # Record the expressions first. Reuse an existing deps constant only when
    # every use is a deps field and needs exactly the same providers.
    edits,groups=[],[]
    visited=set()
    for target,(rule,attrs,additions) in fields.items():
        old=attrs.get('deps')
        if isinstance(old,ast.Name) and old.id in doc.constants:
            same=[(tar,at['deps'],ds) for tar,(_,at,ds) in fields.items()
                  if isinstance(at.get('deps'),ast.Name) and at['deps'].id == old.id]
            if len(same)==references[old.id] and all(ds==additions for _,_,ds in same):
                if old.id in visited:
                    continue
                visited.add(old.id)
                groups.append((doc.constants[old.id],None,additions))
                continue
        groups.append((old,rule,additions))
    # Distinct supported CPUs can use the same actual libraries. A single
    # list preserves //conditions:default=[] without copying libraries twice.
    repeats=Counter(tuple(sorted(values)) for _,_,adds in groups for values in adds.values() if values)
    names={}
    for values,n in repeats.items():
        if n<2:
            continue
        name='_PROTO_LIBRARIES' if not names else '_PROTO_LIBRARIES_'+str(len(names)+1)
        if name in doc.constants:
            raise ValueError(str(doc.path)+': existing '+name+' declaration')
        names[values]=name
    if names:
        before=next(node for node in doc.source.module.body
                    if isinstance(node,ast.Assign)
                    or isinstance(node,ast.Expr) and isinstance(node.value,ast.Call)
                       and any(kw.arg=='name' for kw in node.value.keywords))
        pos=doc.source.span(before)[0]
        decl=''.join(name+' = '+json.dumps([dependency_spelling(r) for r in values],indent=4)+'\n\n'
                     for values,name in names.items())
        edits.append((pos,pos,decl))
    for old,rule,additions in groups:
        if old is not None:
            edits.append((*doc.source.span(old),'('+doc.source.text(old)+') + '+deps_node(additions,names)))
        else:
            end=doc.source.span(rule)[1]-1
            edits.append((end,end,'\n    deps = '+deps_node(additions,names)+',\n'))
    return doc.source.rewrite(edits)


def empty_header_expression(doc,node,constants=frozenset()):
    """Only empty literals and empty CPU selects, never unknown macros/globs."""
    if isinstance(node,ast.Name) and node.id in doc.constants and node.id not in constants:
        return empty_header_expression(doc,doc.constants[node.id],constants|{node.id})
    if isinstance(node,(ast.List,ast.Tuple)):
        return not node.elts
    if isinstance(node,ast.BinOp) and isinstance(node.op,ast.Add):
        return empty_header_expression(doc,node.left,constants) and empty_header_expression(doc,node.right,constants)
    if isinstance(node,ast.Call) and getattr(node.func,'id','')=='select' and node.args and isinstance(node.args[0],ast.Dict):
        table=node.args[0]
        keys={k.value for k in table.keys if isinstance(k,ast.Constant)}
        cpus={'@platforms//cpu:'+cpu for cpu in ARCHES}
        return (len(keys)==len(table.keys) and keys <= cpus|{'//conditions:default'}
                and all(empty_header_expression(doc,n,constants) for n in table.values))
    return False


def drop_emptied_fields(doc, originally, candidates):
    """Drop emptied proto attrs only if another attr retains any CPU guard."""
    edits, constants=[],set()
    for _,rule in doc.source.rules():
        attrs={k.arg:k.value for k in rule.keywords}
        if getattr(rule.func,'id','') not in CC_COMPILERS or not isinstance(attrs.get('name'),ast.Constant):
            continue
        target=doc.reference(':'+attrs['name'].value)
        old=originally.rules.get(target)
        if old is None:
            continue
        original_doc,_,old_attrs=old
        changed=[]
        for field in ('hdrs','textual_hdrs','additional_compiler_inputs'):
            expr=attrs.get(field)
            if (expr is not None and empty_header_expression(doc,expr)
                    and any(candidates.get((target,cpu,field)) for cpu in ARCHES)
                    and expanded(expr,doc)!=expanded(old_attrs.get(field),original_doc)):
                changed.append(field)
        if not changed:
            continue
        # Never drop the only select that rejects an unsupported CPU.
        if (any(no_match_selects(doc,attrs[f]) for f in changed)
                and not any(no_match_selects(doc,n) for f,n in attrs.items() if f not in changed)):
            continue
        for kw in rule.keywords:
            if kw.arg not in changed:
                continue
            first,last=doc.source.span(kw)
            comma=doc.source.bytes[last:doc.source.span(rule)[1]].find(b',')
            if comma<0:
                raise ValueError('expected comma after '+kw.arg)
            edits.append((first,last+comma+1,''))
            constants.update(n.id for n in ast.walk(kw.value) if isinstance(n,ast.Name) and n.id in doc.constants)
    text=doc.source.rewrite(edits)
    tmp=Document(doc.path,text=text,package=doc.package,files=doc.files)
    # Delete only names from these newly emptied fields, after their last use.
    active=Counter(n.id for n in ast.walk(tmp.source.module)
                   if isinstance(n,ast.Name) and isinstance(n.ctx,ast.Load))
    erased=[]
    for node in tmp.source.module.body:
        if (isinstance(node,ast.Assign) and len(node.targets)==1 and isinstance(node.targets[0],ast.Name)
                and node.targets[0].id in constants and not active[node.targets[0].id]):
            erased.append((*tmp.source.span(node),''))
    return tmp.source.rewrite(erased)


def all_proto_requirements(graph,target,cpu,private):
    doc,kind,attrs=graph.rules[target]
    inherited=graph.inherited(target,cpu,private)
    direct=set((field,ref) for field in ('hdrs','textual_hdrs')
               for ref in doc.labels(attrs.get(field),cpu) if generated_proto(ref,cpu,graph))
    if private:
        direct.update(('input',ref) for field in ('srcs','additional_compiler_inputs')
                      for ref in doc.labels(attrs.get(field),cpu) if generated_proto(ref,cpu,graph))
    return direct | {(field,ref) for field,ref in inherited if generated_proto(ref,cpu,graph)}


def no_match_selects(doc,node,constants=frozenset()):
    """Whether an unchanged unsupported CPU can evaluate a known CPU selector."""
    if isinstance(node,ast.Name) and node.id in doc.constants and node.id not in constants:
        return no_match_selects(doc,doc.constants[node.id],constants|{node.id})
    if isinstance(node,ast.Call) and getattr(node.func,'id','')=='select' and node.args and isinstance(node.args[0],ast.Dict):
        table = node.args[0]
        keys = [k.value for k in table.keys if isinstance(k,ast.Constant)]
        cpus = {'@platforms//cpu:'+cpu for cpu in ARCHES}
        if len(keys)==len(table.keys) and set(keys) <= cpus | {'//conditions:default'}:
            if '//conditions:default' not in keys:
                return True
            return no_match_selects(doc,table.values[keys.index('//conditions:default')],constants)
    return any(no_match_selects(doc,x,constants) for x in ast.iter_child_nodes(node)) if isinstance(node,ast.AST) else False


def verify_graph(before, after, planned, candidates):
    counts=Counter()
    if set(before.rules)!=set(after.rules):
        raise ValueError('declared target set changed')
    for target,(doc,kind,attrs) in before.rules.items():
        newer,kind_after,now=after.rules[target]
        if kind!=kind_after:
            raise ValueError(target+': rule class changed')
        gained=set(now)-set(attrs)
        lost=set(attrs)-set(now)
        if gained-{'deps'} or lost-{'hdrs','textual_hdrs','additional_compiler_inputs'}:
            raise ValueError(target+': attribute schema changed')
        if (gained or lost) and (kind not in CC_COMPILERS or doc.repo!='@clickhouse'):
            raise ValueError(target+': non-native compiler attribute schema changed')
        for field in set(attrs)|set(now):
            if kind in CC_COMPILERS and doc.repo=='@clickhouse' and field in PROTECTED_KINDS:
                continue
            if expanded(attrs.get(field),doc)!=expanded(now.get(field),newer):
                raise ValueError(f'{target}: changed protected {field}')
        if kind not in CC_COMPILERS:
            continue
        old_failed=any(no_match_selects(doc,n) for n in attrs.values())
        new_failed=any(no_match_selects(newer,n) for n in now.values())
        if old_failed != new_failed:
            raise ValueError(f'{target}: unsupported CPU matching changes')
        # Every source keeps exact order, including unknown expressions/globs.
        if expanded(attrs.get('srcs'),doc)!=expanded(now.get('srcs'),newer):
            raise ValueError(f'{target}: source ordering or source expression changed')
        old_deps_other=[doc.reference(s) for s in doc.values(attrs.get('deps'),OTHER_CPU)]
        new_deps_other=[newer.reference(s) for s in newer.values(now.get('deps'),OTHER_CPU)]
        if old_deps_other!=new_deps_other:
            raise ValueError(f'{target}: changed unsupported CPU dependencies {old_deps_other!r} to {new_deps_other!r}')
        for cpu in ARCHES:
            old_deps=[doc.reference(s) for s in doc.values(attrs.get('deps'),cpu)]
            new_deps=[newer.reference(s) for s in newer.values(now.get('deps'),cpu)]
            allowed=sorted(planned.get((target,cpu),()))
            if new_deps!=old_deps+allowed:
                raise ValueError(f'{target}: unexpected {cpu} deps')
            for field in HEADER_FIELDS - {'srcs'}:
                was=doc.labels(attrs.get(field),cpu)
                nxt=newer.labels(now.get(field),cpu)
                if nxt-was:
                    raise ValueError(f'{target}: unexpected added {cpu} {field}')
                removed=was-nxt
                if removed - candidates.get((target,cpu,field),set()):
                    raise ValueError(f'{target}: removed unproved {cpu} {field}')
                inherited=after.inherited(target,cpu,compile_only(field))
                for ref in removed:
                    if not inherited & typed_options(field,ref):
                        raise ValueError(f'{target}: lost witness {cpu} {field} {ref}')
                    counts['removed_'+field]+=1
            for private in (False,True):
                # Public hdr kind must stay the same. Private inputs may now
                # arrive as hdrs or textual_hdrs via CcInfo instead of "input".
                old_files=all_proto_requirements(before,target,cpu,private)
                new_files=all_proto_requirements(after,target,cpu,private)
                missing={(f,r) for f,r in old_files-new_files
                         if f!='input' or not new_files & {('hdrs',r),('textual_hdrs',r)}}
                if missing:
                    raise ValueError(f'{target}: lost {cpu} public={not private} {missing}')
            for dep in allowed:
                trail=after.reaches(dep,target,cpu)
                if trail:
                    raise ValueError(f'{target} creates link/compile cycle {trail}')
            counts['cpu_rules']+=1
    counts['planned_owner_deps']=sum(map(len,planned.values()))
    return counts


def transfer(root, output=None, write=False):
    docs=snapshot(root)
    old=BorrowingGraph(root,docs)
    proposal=BorrowingGraph(root,docs)
    candidates,planned,decisions,witnesses,compiled=plan_transfers(proposal)
    texts={}
    next_docs=[]
    literal_count=0
    for doc in docs:
        if doc.repo!='@clickhouse':
            next_docs.append(doc); continue
        cleanup=ProtoRewrite(doc,proposal,candidates)
        result=cleanup.text()
        literal_count+=cleanup.stats['inherited_source_header_literals']
        mid=Document(doc.path,text=result,package=doc.package,files=doc.files)
        result=append_deps(mid,planned)
        mid=Document(doc.path,text=result,package=doc.package,files=doc.files)
        result=drop_emptied_fields(mid,old,candidates)
        if result != doc.source.bytes.decode():
            result=subprocess.run(['buildifier','-config='+str(root/'.buildifier.json'),'-type=build'],input=result,text=True,
                                  stdout=subprocess.PIPE,check=True).stdout
        final=Document(doc.path,text=result,package=doc.package,files=doc.files)
        next_docs.append(final)
        if result != doc.source.bytes.decode():
            texts[doc.path]=(doc,result)
    result_graph=BorrowingGraph(root,next_docs)
    summary=verify_graph(old,result_graph,planned,candidates)
    rerun=BorrowingGraph(root,next_docs)
    again,new_deps,new_decisions,_,_=plan_transfers(rerun)
    if any(new_deps.values()):
        raise ValueError('proto dependencies are not at fixed point')
    for doc in next_docs:
        if doc.repo!='@clickhouse':
            continue
        rewritten=ProtoRewrite(doc,rerun,again).text()
        if rewritten != doc.source.bytes.decode():
            raise ValueError('not idempotent: '+str(doc.path))
    # One verified output receipt per generated input action. All files that
    # describe protoc, plugins, output names, or link refs are outside writes.
    if old.outputs != result_graph.outputs:
        raise ValueError('protoc or other generated output names/producers changed')
    _,new_compiled=find_owners(result_graph)
    if dict(compiled)!=dict(new_compiled):
        raise ValueError('protobuf compilation owners changed')
    summary.update(dict(files=len(texts),literal_refs=literal_count,
                        proto_translation_units=len(compiled)))
    snapshot_sum=hashlib.sha256()
    for doc in sorted(docs,key=lambda d:str(d.path)):
        snapshot_sum.update(str(doc.path.relative_to(root)).encode()+b'\0'+doc.source.bytes)
    report={'summary':dict(summary),'decisions':dict(decisions),'input_build_sha256':snapshot_sum.hexdigest(),
            'rewritten':[str(p.relative_to(root)) for p in sorted(texts)],
            'added_deps':{target+' ['+cpu+']':sorted(v) for (target,cpu),v in planned.items() if v},
            'witnesses':{f'{t} [{cpu}] {f} {r}':owner for (t,cpu,f,r),owner in witnesses.items()}}
    if output is not None:
        for path,(_,content) in texts.items():
            dest=output/path.relative_to(root)
            dest.parent.mkdir(parents=True,exist_ok=True)
            dest.write_text(content)
        output.mkdir(parents=True,exist_ok=True)
        (output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'summary':summary,'decisions':decisions,'output':str(output),'write':write},indent=2))
    if write:
        # This protects overlapping active writers: no partial apply.
        for doc in docs:
            if doc.path.read_bytes()!=doc.source.bytes:
                raise ValueError('BUILD changed during proto audit: '+str(doc.path))
        for path,(_,content) in texts.items():
            path.write_text(content)
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=ROOT)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--write',action='store_true')
    opts=parser.parse_args()
    root=opts.root.resolve()
    out=opts.output.resolve() if opts.output else None
    if out is not None and (out==root or root in out.parents):
        parser.error('--output must be outside ClickHouse')
    transfer(root,out,opts.write)


if __name__=='__main__':
    main()

#!/usr/bin/env python3
"""Publish source include roots from libraries already visible to consumers."""
import argparse
import ast
from collections import Counter, defaultdict
from functools import cache
import gzip
import hashlib
import json
from pathlib import Path
import posixpath
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / 'bazel'))
from initial_migration.compact_build import Source
from initial_migration.native_include_deps import (
    PublicIncludes, active_cpus, canonical_dir, expanded_rules, prove_unchanged, CPUS,
)
from initial_migration.native_public_includes import list_elements
from initial_migration.prune_inherited_headers import CC_COMPILERS, CC_PROVIDERS, Document, SourceDocument
from initial_migration.native_cc_inputs import SPECIAL_REPOS
from initial_migration.repository_inputs import REPOSITORIES, package_sources
from initial_migration.simplify_globs import GlobFiles


def files_and_documents(root, receipts=None, texts=None, inventories=None, *, read_only=()):
    names = subprocess.check_output([
        'git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard', '--', '*.bazel',
        '*.h', '*.hpp', '*.hh', '*.hxx', '*.h++', '*.inc', '*.inl', '*.ipp', '*.tcc', '*.tpp',
        '*.def', '*.defs', '*.th',
    ], cwd=root).decode().split('\0')
    names = tuple(dict.fromkeys(n for n in names if n and (root/n).is_file()))
    paths = [root/name for name in names if Path(name).name == 'BUILD.bazel'
             or name.endswith('.BUILD.bazel')]
    packages = {p.parent.relative_to(root).as_posix().removeprefix('.')
                for p in paths if p.name == 'BUILD.bazel'}
    by_package = defaultdict(list)
    for name in names:
        if name.endswith('.bazel'):
            continue
        parts = name.split('/')
        package = next((posixpath.join(*parts[:i]) for i in range(len(parts)-1, 0, -1)
                        if posixpath.join(*parts[:i]) in packages), '')
        by_package[package].append(name.removeprefix(package + '/') if package else name)
    documents = []
    for path in paths:
        pkg = path.parent.relative_to(root).as_posix().removeprefix('.') if path.name == 'BUILD.bazel' else ''
        doc = (SourceDocument if path in read_only else Document)(path, package=pkg, text=(texts or {}).get(path))
        inventory = None
        if doc.repo == '@clickhouse':
            inventory = by_package[pkg]
        elif doc.repo not in SPECIAL_REPOS and doc.owner in REPOSITORIES:
            if inventories is not None and doc.owner in inventories:
                inventory = inventories[doc.owner]
            elif receipts is None:
                # generate_build has already populated the pinned archive
                # inventory. Standalone callers can supply v3 receipts.
                inventory = package_sources(doc.owner)
            spec = REPOSITORIES[doc.owner]
            receipt = (receipts/(spec['name'] + '-v3-' + spec['sha256'] + '.json.gz')
                       if receipts is not None else None)
            if inventory is None and receipt is not None and receipt.is_file():
                with gzip.open(receipt, 'rt') as stream:
                    data = json.load(stream)
                if (data['owner'] != doc.owner or data['sha256'] != spec['sha256']
                        or data['commit'] != spec['commit']):
                    raise ValueError('Receipt no longer matches archive: ' + str(receipt))
                inventory = data['inventory']
        if inventory is not None:
            doc.globs, doc.files = GlobFiles(inventory), frozenset(inventory)
        documents.append(doc)
    return documents


class Owners(PublicIncludes):
    @cache
    def visible(self, target, cpu):
        doc, kind, attrs = self.rules[target]
        pending = list(self.dependencies(target, cpu))
        if kind in CC_COMPILERS:
            pending.extend(doc.labels(attrs.get('implementation_deps'), cpu))
        seen = {target}
        while pending:
            ref = pending.pop()
            if ref in seen or ref not in self.rules:
                continue
            seen.add(ref)
            if self.rules[ref][1] in CC_PROVIDERS | {'alias'}:
                pending.extend(self.dependencies(ref, cpu))
        return frozenset(seen - {target})

    @cache
    def public_source_files(self, target, cpu):
        doc, kind, attrs = self.rules[target]
        if doc.files is None or kind not in CC_PROVIDERS:
            return frozenset()
        prefix = doc.reference(':')
        declared = doc.labels(attrs.get('hdrs'), cpu) | doc.labels(attrs.get('textual_hdrs'), cpu)
        return frozenset(h for h in declared if h.startswith(prefix)
                         and h[len(prefix):] in doc.files)

    @cache
    def candidates(self, target, cpu, directory):
        if directory.startswith(('bazel/', 'rust/', 'contrib/sysroot', 'contrib/llvm-project')):
            return ()
        consumer, _, _ = self.rules[target]
        candidates = []
        for ref in self.visible(target, cpu):
            doc, kind, attrs = self.rules[ref]
            root = posixpath.normpath(posixpath.join(doc.owner or '', doc.package or ''))
            if (kind not in CC_PROVIDERS or doc.repo in SPECIAL_REPOS or root == '.'
                    or directory != root and not directory.startswith(root + '/')
                    or (doc.repo == consumer.repo and doc.package == consumer.package)):
                continue
            relative = posixpath.relpath(directory, root)
            if relative == 'config' or relative.startswith(('config/', 'gen/')):
                continue
            # A compiled external repository root publishes too many unrelated
            # headers. Require its public subdirectory, or a header-only API.
            if relative == '.' and doc.repo != '@clickhouse' and (
                    kind != 'cc_library' or 'srcs' in attrs):
                continue
            published = self.public_source_files(ref, cpu)
            prefix = doc.reference(':') + (relative.rstrip('/') + '/' if relative != '.' else '')
            matching = [h for h in published if h.startswith(prefix)]
            if not matching:
                continue
            name = ref.partition(':')[2]
            header_api = kind == 'cc_library' and 'srcs' not in attrs
            candidates.append((not header_api, not name.endswith('_headers'), '__compile_' in name,
                               len(published), ref, relative, tuple(sorted(matching)[:3])))
        return tuple(sorted(candidates))


def cpp_fields(text):
    return expanded_rules(text)


def prove_publication(before, after, filename):
    from initial_migration.compact_build import ast_key
    old, new = cpp_fields(before), cpp_fields(after)
    if set(old) != set(new):
        raise ValueError('Target changes during include publication: ' + str(filename))
    for name, (kind, attrs) in old.items():
        nkind, nattrs = new[name]
        ignore = {'includes'} if kind in CC_PROVIDERS else set()
        if kind != nkind or set(attrs)-ignore != set(nattrs)-ignore:
            raise ValueError(f'{filename} {name}: unexpected attribute changes')
        for field in set(attrs)-ignore:
            if ast_key(attrs[field]) != ast_key(nattrs[field]):
                raise ValueError(f'{filename} {name}: changed {field}')


def publish(root, receipts=None, output=None, system_only=False, *, inventories=None, write=False, read_only=()):
    docs = files_and_documents(root, receipts, inventories=inventories, read_only=read_only)
    graph = Owners(docs)
    additions = defaultdict(lambda: defaultdict(set))
    witnesses = []
    counts = Counter()
    for target, (doc, kind, attrs) in graph.rules.items():
        if doc.path in read_only or kind not in CC_COMPILERS or doc.repo in SPECIAL_REPOS or 'local_includes' not in attrs:
            continue
        if system_only and target != '@clickhouse//src/Storages/System:clickhouse_storages_system__compile_1':
            continue
        for cpu in active_cpus(doc, attrs, {'local_includes'}):
            already = graph.available(target, cpu)
            for item in list_elements(attrs['local_includes'], doc, cpu):
                directory = canonical_dir(item, doc)
                if directory is None or directory in already:
                    continue
                choices = [choice for choice in graph.candidates(target, cpu, directory)
                           if graph.rules[choice[-3]][0].path not in read_only]
                if not choices:
                    counts['no_existing_public_source_owner'] += 1
                    continue
                *_, provider, relative, examples = choices[0]
                additions[provider][cpu].add(relative)
                witnesses.append({'consumer':target, 'cpu':cpu, 'directory':directory,
                                  'provider':provider, 'relative':relative, 'examples':list(examples)})
    wanted = {}
    for doc in docs:
        edits = []
        for _, call in doc.source.rules():
            attrs = {kw.arg:kw.value for kw in call.keywords}
            name = attrs.get('name')
            if not isinstance(name, ast.Constant) or not isinstance(name.value, str):
                continue
            target = doc.reference(':' + name.value)
            needed = additions.get(target)
            if not needed:
                continue
            per_cpu = {cpu: sorted(needed.get(cpu, ())) for cpu in CPUS}
            if per_cpu['x86_64'] == per_cpu['aarch64']:
                # Equal source roots are independent of target CPU. No error
                # selector is added to an owner that previously configured.
                addition = json.dumps(sorted(set().union(*needed.values())), indent=4)
            else:
                choices = {'@platforms//cpu:x86_64':per_cpu['x86_64'],
                           '@platforms//cpu:aarch64':per_cpu['aarch64'],
                           '//conditions:default':per_cpu['default']}
                addition = 'select(' + json.dumps(choices, indent=4) + ')'
            if 'includes' in attrs:
                node = attrs['includes']
                replacement = doc.source.text(node) + ' + ' + addition
                edits.append((*doc.source.span(node), replacement))
            else:
                at = doc.source.span(call)[1]-1
                edits.append((at, at, '\n    includes = ' + addition + ',\n'))
        if not edits:
            continue
        after = doc.source.rewrite(edits)
        after = subprocess.run(['buildifier','-config='+str(root/'.buildifier.json'), '-type=build'],
                               input=after, text=True, stdout=subprocess.PIPE, check=True).stdout
        prove_publication(doc.source.bytes.decode(), after, doc.path)
        wanted[doc.path] = after
    actual = Owners(files_and_documents(root, receipts, wanted, inventories, read_only=read_only))
    for row in witnesses:
        if actual.available(row['consumer'],row['cpu']).get(row['directory']) is None:
            raise ValueError('Directory not supplied after publication ' + json.dumps(row))
        if row['provider'] not in actual.visible(row['consumer'],row['cpu']):
            raise ValueError('Provider no longer visible ' + json.dumps(row))
        if not set(row['examples']) <= actual.public_source_files(row['provider'],row['cpu']):
            raise ValueError('Source headers not supplied after publication ' + json.dumps(row))
    report = {
        'scope':'system' if system_only else 'repo', 'builds':len(docs),
        'counts':dict(counts), 'owner_targets':len(additions),
        'added_directories':sum(len(set().union(*v.values())) for v in additions.values()), 'witnesses':witnesses,
        'publish':{t:{cpu:sorted(d.get(cpu,())) for cpu in CPUS} for t,d in sorted(additions.items())},
        'files':[], 'checks':{'pinned_source_ownership': True,'unchanged_expanded_other_attrs':True,
                             'actual_include_reachability':True, 'buildifier':True},
    }
    for doc in docs:
        if doc.path not in wanted:
            continue
        rel = doc.path.relative_to(root)
        before = doc.source.bytes.decode()
        after = wanted[doc.path]
        if output is not None:
            dest=output/rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(after)
        report['files'].append({'path':str(rel),'before_sha256':hashlib.sha256(before.encode()).hexdigest(),
                                'after_sha256':hashlib.sha256(after.encode()).hexdigest()})
    # Publication is idempotent: every proposal has a final reachable owner.
    for row in witnesses:
        choices = actual.available(row['consumer'], row['cpu'])
        if row['directory'] not in choices:
            raise ValueError('Publication did not reach a fixed point: ' + json.dumps(row))
    if write:
        for doc in docs:
            if doc.path in wanted and doc.path.read_bytes() != doc.source.bytes:
                raise ValueError('BUILD changed during publication proof: ' + str(doc.path))
        for path, after in wanted.items():
            path.write_text(after)
    if output is not None:
        output.mkdir(parents=True,exist_ok=True)
        (output/'report.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({key:value for key,value in report.items() if key not in {'witnesses','files','publish'}},indent=2))
    return report, wanted


if __name__ == '__main__':
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--root',type=Path,default=ROOT)
    ap.add_argument('--receipts',type=Path,default=Path('work/include-adjacency'))
    ap.add_argument('--output',type=Path,required=True)
    ap.add_argument('--system-only',action='store_true')
    ap.add_argument('--write',action='store_true')
    args=ap.parse_args()
    publish(args.root,args.receipts,args.output,system_only=args.system_only,write=args.write)

#!/usr/bin/env python3
"""Remove explicit headers that existing native cc_library dependencies publish.

This updates BUILD files produced before BuildFile.dependency_headers().
Literal file labels are compared, and --inventory resolves globs at pinned
source archive revisions. Unknown expressions never establish coverage.
No deps, source order, includes, or compiler options are changed.
"""

import argparse
import ast
from collections import Counter
from functools import cache
import gzip
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.compact_build import Source, ast_key
from initial_migration.repository_inputs import CC_HEADER_SUFFIXES, REPOSITORIES, ROOT
from initial_migration.simplify_globs import GlobFiles, _literal_strings

ARCHES = ('x86_64', 'aarch64')
CC_PROVIDERS = {'cc_library', 'ch_cc_library', 'cc_import'}
CC_COMPILERS = CC_PROVIDERS | {'cc_binary', 'cc_test'}
# Header files in cc_library srcs are private compile inputs, not translation
# units. Remove such a header only after the same source arrives from deps.
HEADER_FIELDS = {'srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs'}
POSTGRES_HEADERS = (
    '@clickhouse_postgres//:postgres_headers',
    '@clickhouse_libpqxx//:libpqxx_headers',
)


def label(value, repo):
    if value.startswith(':'):
        return repo + '//' + value
    if value.startswith('//'):
        return repo + value
    return value if value.startswith('@') else repo + '//:' + value


class Document:
    def __init__(self, path, inventories=None):
        self.path = path
        self.source = Source(path.read_text())
        self.repo = '@clickhouse' if path.name == 'BUILD.bazel' else (
            '@' + path.name.removesuffix('.BUILD.bazel'))
        self.constants = {
            statement.targets[0].id: statement.value for statement in self.source.module.body
            if isinstance(statement, ast.Assign) and len(statement.targets) == 1
            and isinstance(statement.targets[0], ast.Name)
        }
        self.owner = next((owner for owner, spec in REPOSITORIES.items()
                           if '@' + spec['name'] == self.repo), None)
        inventory = (inventories or {}).get(self.owner)
        self.files = frozenset(inventory) if inventory is not None else None
        self.globs = GlobFiles(inventory) if inventory is not None else None
        self.glob_cache = {}

    def values(self, node, arch):
        """Labels guaranteed on arch, even if other config settings change."""
        if isinstance(node, ast.Constant):
            return [node.value] if isinstance(node.value, str) else []
        if isinstance(node, ast.Name):
            return self.values(self.constants.get(node.id), arch)
        if isinstance(node, (ast.List, ast.Tuple)):
            return [value for item in node.elts for value in self.values(item, arch)]
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return self.values(node.left, arch) + self.values(node.right, arch)
        if (isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'select'
                and len(node.args) == 1 and isinstance(node.args[0], ast.Dict)):
            choices = node.args[0]
            if not all(isinstance(key, ast.Constant) and isinstance(key.value, str) for key in choices.keys):
                return []
            table = {key.value: val for key, val in zip(choices.keys, choices.values)}
            if set(table) <= {'@platforms//cpu:' + cpu for cpu in ARCHES} | {'//conditions:default'}:
                return self.values(table.get('@platforms//cpu:' + arch, table.get('//conditions:default')), arch)
            branches = [set(self.values(val, arch)) for val in table.values()]
            return sorted(set.intersection(*branches)) if branches else []
        if (self.globs is not None and isinstance(node, ast.Call)
                and getattr(node.func, 'id', '') == 'glob' and len(node.args) == 1):
            key = ast_key(node)
            if key not in self.glob_cache:
                opts = {kw.arg: kw.value for kw in node.keywords}
                patterns = _literal_strings(node.args[0])
                excludes = _literal_strings(opts['exclude']) if 'exclude' in opts else []
                if patterns is None or excludes is None or set(opts) - {'exclude', 'allow_empty'}:
                    self.glob_cache[key] = []
                else:
                    self.glob_cache[key] = [':' + name for name in self.globs.result(patterns, excludes)]
            return self.glob_cache[key]
        return []

    def labels(self, node, arch):
        return {label(value, self.repo) for value in self.values(node, arch)}

class Graph:
    def __init__(self, documents):
        self.rules = {}
        self.documents = {doc.repo: doc for doc in documents}
        self.extra_deps = {}
        for doc in documents:
            for _, call in doc.source.rules():
                attrs = {kw.arg: kw.value for kw in call.keywords}
                name = attrs['name']
                if isinstance(name, ast.Constant) and isinstance(name.value, str):
                    self.rules[label(':' + name.value, doc.repo)] = (
                        doc, getattr(call.func, 'id', ''), attrs)

    @cache
    def dependencies(self, target, arch):
        rule = self.rules.get(target)
        if rule is None:
            return ()
        doc, kind, attrs = rule
        if kind == 'alias':
            return tuple(doc.labels(attrs.get('actual'), arch))
        if kind not in CC_COMPILERS:
            return ()
        return tuple(doc.labels(attrs.get('deps'), arch) | self.extra_deps.get(target, set()))

    def add_postgres_header_deps(self, consumers):
        """Replace captured PostgreSQL header files with owner-side libraries."""
        updates = {}
        for target, (doc, kind, attrs) in self.rules.items():
            if kind not in CC_COMPILERS or doc.repo not in consumers:
                continue
            for provider in POSTGRES_HEADERS:
                if provider not in self.rules or provider.partition('//:')[0] == doc.repo:
                    continue
                for arch in ARCHES:
                    published = self.own_headers(provider, arch)
                    if not published:
                        continue
                    for field in HEADER_FIELDS:
                        raw = doc.labels(attrs.get(field), arch) & published
                        inherited = self.inherited(target, arch, field in {'srcs', 'additional_compiler_inputs'})
                        if raw - inherited:
                            updates.setdefault(target, set()).add(provider)
        self.extra_deps = updates
        self.dependencies.cache_clear()
        self.inherited.cache_clear()

    @cache
    def own_headers(self, target, arch):
        rule = self.rules.get(target)
        if rule is None:
            return frozenset()
        doc, kind, attrs = rule
        if kind not in CC_PROVIDERS:
            return frozenset()
        headers = set().union(*(doc.labels(attrs.get(field), arch)
                                for field in ('hdrs', 'textual_hdrs')))
        # Anchor removal to the actual owner rather than another consumer's
        # copy of the same foreign header. Named rules may generate multiple
        # files; a rule label never proves an exact source-header declaration.
        prefix = doc.repo + '//:'
        return frozenset(header for header in headers
                         if header.startswith(prefix) and header not in self.rules
                         and (doc.files is None or header.removeprefix(prefix) in doc.files))

    @cache
    def inherited(self, target, arch, compile_only=False):
        headers, seen = set(), {target}
        pending = list(self.dependencies(target, arch))
        if compile_only and target in self.rules:
            doc, kind, attrs = self.rules[target]
            if kind in CC_COMPILERS:
                pending.extend(doc.labels(attrs.get('implementation_deps'), arch))
        while pending:
            dependency = pending.pop()
            if dependency in seen:
                continue
            seen.add(dependency)
            headers.update(self.own_headers(dependency, arch))
            # A dependency's implementation_deps are not published in CcInfo.
            if self.rules.get(dependency, (None, None, None))[1] in CC_PROVIDERS | {'alias'}:
                pending.extend(self.dependencies(dependency, arch))
        return frozenset(headers)


class Rewrite:
    def __init__(self, doc, graph, owners=()):
        self.doc = doc
        self.graph = graph
        self.source = doc.source
        self.stats = Counter()
        self.owners = set(owners)
        # One decision per literal location. If a constant has multiple
        # consumers, every consumer must inherit that literal before removal.
        self.remove = {}
        self.lists = {}
        self.seen = set()
        self.removed_by_owner = Counter()

    def covered(self, value, target, arches, field):
        candidate = label(value, self.doc.repo)
        if not arches or candidate in self.graph.rules:
            return False
        repo, separator, filename = candidate.partition('//:')
        owner = self.graph.documents.get(repo)
        # Generated or unknown sources and this repository's public headers
        # retain their original declarations.
        if (not separator or repo == self.doc.repo or owner is None
                or self.owners and repo not in self.owners
                or owner.files is None or filename not in owner.files):
            return False
        if not candidate.endswith((*CC_HEADER_SUFFIXES, '.th', '.defs')):
            # libpqxx's published extensionless wrappers contain #include
            # directives and are declared as textual_hdrs by their owner.
            provider = '@clickhouse_libpqxx//:libpqxx_headers'
            if (repo != '@clickhouse_libpqxx' or not filename.startswith('include/pqxx/')
                    or '.' in filename.removeprefix('include/pqxx/')
                    or not all(candidate in self.graph.own_headers(provider, cpu) for cpu in arches)):
                return False
        return all(candidate in self.graph.inherited(
            target, cpu, field in {'srcs', 'additional_compiler_inputs'}) for cpu in arches)

    def walk(self, node, target, arches, field, constants=()):
        if node is None:
            return
        if isinstance(node, ast.Name):
            if node.id in self.doc.constants and node.id not in constants:
                key = (node.id, target, arches, field)
                if key in self.seen:
                    return
                self.seen.add(key)
                self.walk(self.doc.constants[node.id], target, arches, field, (*constants, node.id))
            return
        if isinstance(node, (ast.List, ast.Tuple)):
            for item in node.elts:
                if isinstance(item, ast.Constant) and isinstance(item.value, str):
                    self.lists[id(item)] = (node, item)
                    may_remove = (field in HEADER_FIELDS
                                  and self.covered(item.value, target, arches, field))
                    self.remove[id(item)] = self.remove.get(id(item), True) and may_remove
                else:
                    self.walk(item, target, arches, field, constants)
            return
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            self.walk(node.left, target, arches, field, constants)
            self.walk(node.right, target, arches, field, constants)
            return
        if (isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'select'
                and len(node.args) == 1 and isinstance(node.args[0], ast.Dict)):
            choices = node.args[0]
            keys = {k.value for k in choices.keys if isinstance(k, ast.Constant)}
            cpus = {'@platforms//cpu:' + cpu for cpu in ARCHES}
            cpu_only = len(keys) == len(choices.keys) and keys <= cpus | {'//conditions:default'}
            for key, val in zip(choices.keys, choices.values):
                selected = arches
                if cpu_only:
                    selected = tuple(cpu for cpu in arches
                                     if key.value == '@platforms//cpu:' + cpu
                                     or key.value == '//conditions:default'
                                     and '@platforms//cpu:' + cpu not in keys)
                self.walk(val, target, selected, field, constants)
            return
        # Don't edit any other expression's string arguments: glob patterns,
        # string formatting, macro kwargs and generated-rule inputs stay exact.
        for child in ast.iter_child_nodes(node):
            self.walk(child, target, arches, None, constants)

    def text(self):
        edits = []
        for statement in self.source.module.body:
            if isinstance(statement, ast.Assign):
                # Constants are evaluated at each actual consumer, including
                # non-header attributes that prohibit a shared-list edit.
                continue
            if (isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call)
                    and isinstance(statement.value.func, ast.Name)):
                call = statement.value
                attrs = {kw.arg: kw.value for kw in call.keywords}
                name = attrs.get('name')
                if (call.func.id in CC_COMPILERS and isinstance(name, ast.Constant)
                        and isinstance(name.value, str)):
                    target = label(':' + name.value, self.doc.repo)
                    for key, val in attrs.items():
                        self.walk(val, target, ARCHES, key if key in HEADER_FIELDS else None)
                    for arg in call.args:
                        self.walk(arg, target, ARCHES, None)
                    additional = self.graph.extra_deps.get(target)
                    if additional:
                        new = json.dumps(sorted(additional), indent=4)
                        if 'deps' in attrs:
                            node = attrs['deps']
                            edits.append((*self.source.span(node), '(' + self.source.text(node) + ') + ' + new))
                        else:
                            position = self.source.span(call)[1] - 1
                            edits.append((position, position, '\n    deps = ' + new + ',\n'))
                        self.stats['header_provider_deps'] += len(additional)
                    continue
            self.walk(statement, None, ARCHES, None)

        literals = 0
        for ident, yes in self.remove.items():
            if not yes:
                continue
            node, item = self.lists[ident]
            first, last = self.source.span(item)
            index = node.elts.index(item)
            end = self.source.span(node.elts[index + 1])[0] if index + 1 < len(node.elts) else self.source.span(node)[1] - 1
            comma = self.source.bytes[last:end].find(b',')
            if comma >= 0:
                last += comma + 1
            edits.append((first, last, ''))
            literals += 1
            repo = label(item.value, self.doc.repo).partition('//:')[0]
            self.removed_by_owner[repo] += 1
        self.stats['inherited_source_header_literals'] = literals
        return self.source.rewrite(edits)


def prune(root, inventories, *, write=False, consumers=(), owners=(), postgres_header_deps=False):
    """Remove inherited headers using pinned package filenames keyed by contrib owner."""
    docs = [Document(path, inventories) for path in [root / 'BUILD.bazel', *sorted(
        (root / 'bazel/dependencies').glob('*.BUILD.bazel'))]]
    graph = Graph(docs)
    consumers = set(consumers) or {doc.repo for doc in docs}
    if postgres_header_deps:
        graph.add_postgres_header_deps(consumers)
    total = Counter()
    removed_owners = Counter()
    for doc in docs:
        if doc.repo not in consumers:
            continue
        rewrite = Rewrite(doc, graph, owners)
        after = rewrite.text()
        if after != doc.source.bytes.decode():
            ast.parse(after)
            total.update(rewrite.stats)
            removed_owners.update(rewrite.removed_by_owner)
            total['files'] += 1
            print(doc.path.relative_to(root), dict(rewrite.stats))
            if write:
                doc.path.write_text(after)
    print('Total:', dict(total), 'written' if write else 'no files written')
    print('Removed source-header literals by owner:', json.dumps(dict(removed_owners), sort_keys=True))
    return total, removed_owners


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--inventory', action='append', type=Path, default=[],
                        help='gzip JSON of source inventories from SHA-checked pinned archives')
    parser.add_argument('--consumer', action='append', default=[],
                        help='restrict changed consumers to an apparent repository, e.g. @clickhouse')
    parser.add_argument('--owner', action='append', default=[],
                        help='restrict pruned headers to an apparent source repository')
    parser.add_argument('--postgres-header-deps', action='store_true',
                        help='use existing PostgreSQL header-only providers for captured raw headers')
    args = parser.parse_args()
    inventories = {}
    for path in args.inventory:
        with gzip.open(path, 'rt') as source:
            for owner, entry in json.load(source).items():
                if owner not in REPOSITORIES or entry['sha256'] != REPOSITORIES[owner]['sha256']:
                    raise ValueError(f'{path}: inventory is not at the configured SHA256: {owner}')
                inventories[owner] = entry['files']
    prune(args.root, inventories, write=args.write, consumers=args.consumer, owners=args.owner,
          postgres_header_deps=args.postgres_header_deps)


if __name__ == '__main__':
    main()

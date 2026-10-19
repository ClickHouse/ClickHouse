#!/usr/bin/env python3
"""Remove explicit headers that existing native cc_library dependencies publish.

This updates BUILD files produced before BuildFile.dependency_headers().
Only selected, exact file labels are compared; globs and unknown Starlark
expressions never establish that a dependency provides a header. No deps,
source order, includes, or compiler options are changed.
"""

import argparse
import ast
from collections import Counter
from functools import cache
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.compact_build import Source, remove_unused_constants
from initial_migration.repository_inputs import CC_HEADER_SUFFIXES, REPOSITORIES, ROOT

ARCHES = ('x86_64', 'aarch64')
CC_PROVIDERS = {'cc_library', 'ch_cc_library', 'cc_import'}
MONGO_TEXTUAL_HEADERS = {
    '_libbson': ':src/common/src/mlib/vec.th',
    '_libmongoc': ':src/libmongoc/src/mongoc/mongoc-counters.defs',
}


def label(value, repo):
    if value.startswith(':'):
        return repo + '//' + value
    if value.startswith('//'):
        return repo + value
    return value


class Document:
    def __init__(self, path):
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

    def values(self, node, arch):
        """Resolve only literal labels, constants, concatenations and CPU selects."""
        if isinstance(node, ast.Constant):
            return [node.value] if isinstance(node.value, str) else []
        if isinstance(node, ast.Name):
            return self.values(self.constants.get(node.id), arch)
        if isinstance(node, (ast.List, ast.Tuple)):
            return [value for item in node.elts for value in self.values(item, arch)]
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return self.values(node.left, arch) + self.values(node.right, arch)
        if (isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'select'
                and isinstance(node.args[0], ast.Dict)):
            choices = node.args[0]
            for key, value in zip(choices.keys, choices.values):
                if isinstance(key, ast.Constant) and key.value == '@platforms//cpu:' + arch:
                    return self.values(value, arch)
            for key, value in zip(choices.keys, choices.values):
                if isinstance(key, ast.Constant) and key.value == '//conditions:default':
                    return self.values(value, arch)
        return []

    def labels(self, node, arch):
        return {label(value, self.repo) for value in self.values(node, arch)}

    def own_config(self, values):
        """Identify small lists of this repository's checked-in CMake headers."""
        if not self.owner or not values or len(values) > 128:
            return False
        prefix = '@clickhouse//:' + self.owner + '-cmake/'
        return all(value.startswith(prefix) and value.endswith(CC_HEADER_SUFFIXES)
                   for value in values)


class Graph:
    def __init__(self, documents):
        self.rules = {}
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
        if kind not in CC_PROVIDERS:
            return ()
        return tuple(doc.labels(attrs.get('deps'), arch))

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
        name = target.rsplit(':', 1)[-1]
        if doc.repo == '@clickhouse_mongo_c_driver' and name in MONGO_TEXTUAL_HEADERS:
            headers.add(label(MONGO_TEXTUAL_HEADERS[name], doc.repo))
        return frozenset(headers)

    @cache
    def inherited(self, target, arch):
        headers, seen = set(), {target}
        pending = list(self.dependencies(target, arch))
        while pending:
            dependency = pending.pop()
            if dependency in seen:
                continue
            seen.add(dependency)
            headers.update(self.own_headers(dependency, arch))
            pending.extend(self.dependencies(dependency, arch))
        return frozenset(headers)


class Rewrite:
    def __init__(self, doc, graph):
        self.doc = doc
        self.graph = graph
        self.source = doc.source
        self.stats = Counter()
        self.inherited = {}
        self.mongo_inputs = False

    def covered(self, value, arch):
        candidate = label(value, self.doc.repo)
        if self.mongo_inputs:
            return candidate.startswith('@clickhouse_mongo_c_driver//:')
        # Filegroups, generated rules, and toolchain targets may publish more
        # than CcInfo headers. They are not proof about an exact header file.
        if not candidate.endswith((*CC_HEADER_SUFFIXES, '.th', '.defs')):
            return False
        if arch:
            return candidate in self.inherited[arch]
        return all(candidate in self.inherited[cpu] for cpu in ARCHES)

    def prune(self, node, arch=None):
        original = self.source.text(node)
        if isinstance(node, ast.List):
            kept = [item for item in node.elts
                    if not (isinstance(item, ast.Constant) and isinstance(item.value, str)
                            and self.covered(item.value, arch))]
            removed = len(node.elts) - len(kept)
            if not removed:
                return original, 0
            return ('[\n' + ''.join(' ' * (node.col_offset + 4) + self.source.text(item) + ',\n'
                                    for item in kept) + ' ' * node.col_offset + ']'
                    if kept else '[]'), removed
        if isinstance(node, ast.Name) and node.id.startswith('_GENERATED_'):
            definition = self.doc.constants.get(node.id)
            if definition is None:
                return original, 0
            result, removed = self.prune(definition, arch)
            values = self.doc.values(definition, arch or ARCHES[0])
            # Inline short residual declarations owned by this repository,
            # including configurations shared by two CMake compile groups.
            if self.doc.own_config(values):
                return result, removed
            # Retaining a large, partially inherited constant is preferable
            # to expanding thousands of literals at every reference.
            if removed and (result == '[]' or len(values) - removed <= 24):
                return result, removed
            return original, 0
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left, nl = self.prune(node.left, arch)
            right, nr = self.prune(node.right, arch)
            if left == '[]':
                return right, nl + nr
            if right == '[]':
                return left, nl + nr
            if not nl and not nr and left == self.source.text(node.left) and right == self.source.text(node.right):
                return original, 0
            return '(' + left + ' + ' + right + ')', nl + nr
        if (isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'select'
                and isinstance(node.args[0], ast.Dict)):
            choices = node.args[0]
            rows = []
            count = 0
            changed = False
            for key, value in zip(choices.keys, choices.values):
                if not isinstance(key, ast.Constant):
                    return original, 0
                cpu = next((a for a in ARCHES if key.value == '@platforms//cpu:' + a), None)
                result, removed = self.prune(value, cpu)
                changed |= result != self.source.text(value)
                count += removed
                rows.append(' ' * (node.col_offset + 4) + self.source.text(key) + ': ' + result + ',\n')
            if changed:
                return 'select({\n' + ''.join(rows) + ' ' * node.col_offset + '})', count
        return original, 0

    def text(self):
        edits = []
        for _, call in self.source.rules():
            if getattr(call.func, 'id', '') not in {'cc_library', 'ch_cc_library'}:
                continue
            attrs = {kw.arg: kw.value for kw in call.keywords}
            name = attrs['name']
            if not isinstance(name, ast.Constant) or not isinstance(name.value, str):
                continue
            target = label(':' + name.value, self.doc.repo)
            self.inherited = {arch: self.graph.inherited(target, arch) for arch in ARCHES}
            for field in ('hdrs', 'textual_hdrs', 'additional_compiler_inputs'):
                self.mongo_inputs = (field == 'additional_compiler_inputs'
                                     and self.doc.repo == '@clickhouse_mongo_cxx_driver'
                                     and name.value in {'_bsoncxx', '_mongocxx'})
                if field not in attrs or field == 'additional_compiler_inputs' and not self.mongo_inputs:
                    continue
                result, removed = self.prune(attrs[field])
                if result != self.source.text(attrs[field]):
                    edits.append((*self.source.span(attrs[field]), result))
                    self.stats[field] += removed
            if self.doc.repo == '@clickhouse_mongo_c_driver' and name.value in MONGO_TEXTUAL_HEADERS:
                # Initial BUILD files do not declare these nonstandard headers.
                value = MONGO_TEXTUAL_HEADERS[name.value]
                if 'textual_hdrs' not in attrs:
                    start, _ = self.source.span(attrs['name'])
                    start = self.source.bytes.index(b',', start) + 1
                    edits.append((start, start, '\n    textual_hdrs = ["' + value + '"],'))
                elif any(value not in self.doc.values(attrs['textual_hdrs'], arch) for arch in ARCHES):
                    raise ValueError(f'{target}: merge existing textual_hdrs manually')
        result = self.source.rewrite(edits)
        while True:
            cleaned = remove_unused_constants(Source(result))
            if cleaned == result:
                return result
            result = cleaned


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--root', type=Path, default=ROOT)
    args = parser.parse_args()
    docs = [Document(path) for path in [args.root / 'BUILD.bazel', *sorted(
        (args.root / 'bazel/dependencies').glob('*.BUILD.bazel'))]]
    graph = Graph(docs)
    total = Counter()
    for doc in docs:
        rewrite = Rewrite(doc, graph)
        after = rewrite.text()
        if after != doc.source.bytes.decode():
            ast.parse(after)
            total.update(rewrite.stats)
            total['files'] += 1
            print(doc.path.relative_to(args.root), dict(rewrite.stats))
            if args.write:
                doc.path.write_text(after)
    print('Total:', dict(total), 'written' if args.write else 'no files written')


if __name__ == '__main__':
    main()

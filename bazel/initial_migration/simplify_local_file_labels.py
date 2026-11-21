#!/usr/bin/env python3
"""Remove ':' only from same-package native source-file labels.

Pass --facts with scan_include_adjacency.py's v3 receipts. The default is a
dry run. --write edits only the verified colon byte; rules, output labels,
unknown files, globs, and other attributes keep their existing spelling.
"""

import argparse
import ast
from collections import Counter
import copy
import gzip
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.compact_build import ast_key
from initial_migration.prune_inherited_headers import (
    ARCHES, CC_COMPILERS, HEADER_FIELDS, Document, Graph, Rewrite, label,
)
from initial_migration.repository_inputs import REPOSITORIES, ROOT


class LocalFileLabels(Rewrite):
    def __init__(self, doc, graph):
        super().__init__(doc, graph)
        self.outputs = set()
        for _, call in self.source.rules():
            attrs = {kw.arg: kw.value for kw in call.keywords}
            for field in ('name', 'out', 'outs', 'output', 'outputs'):
                for cpu in ARCHES:
                    self.outputs.update(doc.labels(attrs.get(field), cpu))

    def covered(self, value, target, arches, field):
        if not target or not arches or not value.startswith(':') or self.doc.files is None:
            return False
        source = label(value, self.doc.repo)
        return (value[1:] in self.doc.files
                and source not in self.graph.rules and source not in self.outputs)

    def text(self):
        # Rewrite.walk intersects permission at every use of a shared
        # constant. A use in deps, a generator, or another attribute prevents
        # a source-only change to that constant.
        for statement in self.source.module.body:
            if isinstance(statement, ast.Assign):
                continue
            if (isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call)
                    and isinstance(statement.value.func, ast.Name)):
                call = statement.value
                attrs = {kw.arg: kw.value for kw in call.keywords}
                name = attrs.get('name')
                if (call.func.id in CC_COMPILERS and isinstance(name, ast.Constant)
                        and isinstance(name.value, str)):
                    target = label(':' + name.value, self.doc.repo)
                    for field, value in attrs.items():
                        self.walk(value, target, ARCHES, field if field in HEADER_FIELDS else None)
                    for arg in call.args:
                        self.walk(arg, target, ARCHES, None)
                    continue
            self.walk(statement, None, ARCHES, None)

        edits = []
        permitted = set()
        examples = []
        for ident, allowed in self.remove.items():
            if not allowed:
                continue
            _, item = self.lists[ident]
            first, end = self.source.span(item)
            # Leave unusual quoting or encoded strings unchanged.
            if self.source.bytes[first:first + 2] not in (b'":', b"':"):
                continue
            edits.append((first + 1, first + 2, ''))
            permitted.add((first, end))
            examples.append((item.lineno, item.value))
        after = self.source.rewrite(edits)

        # The complete BUILD AST must be identical except for the proven
        # local-file spelling. This preserves every ordered list, select key,
        # rule, and non-source attribute for all architectures.
        expected = copy.deepcopy(self.source.module)
        for node in ast.walk(expected):
            if isinstance(node, ast.Constant) and self.source.span(node) in permitted:
                node.value = node.value[1:]
        if ast_key(ast.parse(after)) != ast_key(expected):
            raise ValueError('Unexpected BUILD change: ' + str(self.doc.path))
        self.stats['source_file_colons'] = len(edits)
        self.examples = examples
        return after


def simplify(root, facts, *, write=False):
    inventories = {}
    for owner, spec in REPOSITORIES.items():
        path = facts / (spec['name'] + '-v3-' + spec['sha256'] + '.json.gz')
        if not path.is_file():
            continue
        with gzip.open(path, 'rt') as file:
            receipt = json.load(file)
        if (receipt['owner'], receipt['commit'], receipt['sha256'], receipt['version']) != (
                owner, spec['commit'], spec['sha256'], 3):
            raise ValueError('Receipt does not match configured source pin: ' + str(path))
        inventories[owner] = receipt['inventory']
    docs = [Document(path, inventories)
            for path in sorted((root / 'bazel/dependencies').glob('*.BUILD.bazel'))]
    graph = Graph(docs)
    total, details, updates, skipped = Counter(), {}, {}, []
    for doc in docs:
        if doc.files is None:
            skipped.append(doc.repo)
            continue
        rewrite = LocalFileLabels(doc, graph)
        after = rewrite.text()
        if after != doc.source.bytes.decode():
            total.update(rewrite.stats)
            details[doc.path.name] = {'count': rewrite.stats['source_file_colons'],
                                      'examples': rewrite.examples[:6]}
            updates[doc.path] = (doc.source.bytes.decode(), after)
    if write:
        # Validate every result before editing the first BUILD file.
        for path, (before, _) in updates.items():
            if path.read_text() != before:
                raise ValueError('BUILD file changed during audit: ' + str(path))
        for path, (_, after) in updates.items():
            path.write_text(after)
    return {'files': len(updates), 'counts': total, 'details': details,
            'inventory_repos': len(inventories), 'skipped': skipped}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--facts', required=True, type=Path)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    print(json.dumps(simplify(args.root, args.facts, write=args.write), indent=2, sort_keys=True))


if __name__ == '__main__':
    main()

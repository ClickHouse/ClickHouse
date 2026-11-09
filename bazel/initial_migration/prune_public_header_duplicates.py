#!/usr/bin/env python3
"""Remove checked-in headers already published by native cc_library deps.

Use --facts with scan_include_adjacency.py v3 receipts. Only hdrs and
textual_hdrs change. A literal, whole glob, or single glob pattern must resolve
entirely to pinned source files published through deps on both supported CPUs.
implementation_deps never establish coverage. Rule and output labels,
unverified repositories, unknown expressions, and private sources stay intact.

Every native rule's before/after public source-header set is verified on both
CPUs before any file is written. The other BUILD statements must retain their
complete original ASTs. Run without --write for the same checks and a report.
"""

import argparse
import ast
from collections import Counter
import copy
from functools import cache
import gzip
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.compact_build import ast_key
from initial_migration.prune_inherited_headers import (
    ARCHES, CC_COMPILERS, CC_PROVIDERS, Document, Graph, label,
)
from initial_migration.repository_inputs import REPOSITORIES, ROOT
from initial_migration.simplify_globs import _literal_strings

PUBLIC_HEADERS = frozenset({'hdrs', 'textual_hdrs'})
CPU_KEYS = frozenset('@platforms//cpu:' + cpu for cpu in ARCHES)
OUTPUT_FIELDS = ('out', 'outs', 'output', 'outputs')


def inventories_at_pins(facts):
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
    return inventories


class PublicGraph(Graph):
    def __init__(self, documents):
        super().__init__(documents)
        # If a generated output shadows a checked-in file, Bazel's label
        # selects the generated output. Such a label cannot be file proof.
        self.outputs = set()
        for doc, _, attrs in self.rules.values():
            for field in OUTPUT_FIELDS:
                for cpu in ARCHES:
                    self.outputs.update(doc.labels(attrs.get(field), cpu))

    @cache
    def own_headers(self, target, arch):
        rule = self.rules.get(target)
        if rule is None:
            return frozenset()
        doc, _, attrs = rule
        # Prefix attributes create virtual header artifacts. Identical source
        # names alone cannot prove that the virtual artifacts are identical.
        if doc.files is None or 'include_prefix' in attrs or 'strip_include_prefix' in attrs:
            return frozenset()
        return super().own_headers(target, arch) - self.outputs

    def checked_file(self, value, repo):
        candidate = label(value, repo)
        owner, sep, filename = candidate.partition('//:')
        doc = self.documents.get(owner)
        return (bool(sep) and doc is not None and doc.files is not None
                and filename in doc.files and candidate not in self.rules
                and candidate not in self.outputs)


def non_header_statements(doc):
    """Mask only native header attribute values; all other AST bytes matter."""
    module = copy.deepcopy(doc.source.module)
    for statement in module.body:
        if not isinstance(statement, ast.Expr) or not isinstance(statement.value, ast.Call):
            continue
        call = statement.value
        if getattr(call.func, 'id', '') not in CC_COMPILERS:
            continue
        for kw in call.keywords:
            if kw.arg in PUBLIC_HEADERS:
                kw.value = ast.Constant(value='checked public headers')
    return ast_key(module)


class Rewrite:
    def __init__(self, doc, graph):
        self.doc = doc
        self.graph = graph
        self.source = doc.source
        self.counts = Counter()
        self.skipped = Counter()
        self.details = []
        self.partials = []

    def covered(self, values, target, arches):
        return (bool(values) and bool(arches)
                and all(self.graph.checked_file(value, self.doc.repo) for value in values)
                and all(self.doc.labels(ast.List(elts=[ast.Constant(value=v) for v in values]), cpu)
                        <= self.graph.inherited(target, cpu)
                        for cpu in arches))

    def remember(self, node, target, field, kind, values, arches):
        self.counts[kind] += 1
        self.counts['header_declarations'] += len(set(values))
        self.details.append({
            'target': target, 'field': field, 'line': node.lineno, 'kind': kind,
            'headers': len(set(values)), 'arches': list(arches),
        })

    def expression(self, node, target, arches, field):
        original = self.source.text(node)
        if not arches:
            return original
        if isinstance(node, ast.List):
            if '#' in original:
                self.skipped['commented_lists'] += 1
                return original
            retained = []
            for item in node.elts:
                if (isinstance(item, ast.Constant) and isinstance(item.value, str)
                        and self.covered([item.value], target, arches)):
                    self.remember(item, target, field, 'literals', [item.value], arches)
                else:
                    retained.append(self.source.text(item))
            if len(retained) < len(node.elts):
                return '[' + ', '.join(retained) + ']'
            return original
        if isinstance(node, ast.Name):
            # Edit the use, never a shared constant. Mixed constants remain
            # intact rather than expanding them into every consumer.
            definition = self.doc.constants.get(node.id)
            values = _literal_strings(definition)
            if values is not None and self.covered(values, target, arches):
                self.remember(node, target, field, 'constants', values, arches)
                return '[]'
            if definition is not None:
                self.skipped['unknown_or_partial_constants'] += 1
            return original
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = self.expression(node.left, target, arches, field)
            right = self.expression(node.right, target, arches, field)
            if left == '[]':
                return right
            if right == '[]':
                return left
            if (left, right) != (self.source.text(node.left), self.source.text(node.right)):
                return '(' + left + ') + (' + right + ')'
            return original
        if not isinstance(node, ast.Call):
            self.skipped['unknown_expressions'] += 1
            return original
        function = getattr(node.func, 'id', '')
        if (function == 'select' and len(node.args) == 1 and not node.keywords
                and isinstance(node.args[0], ast.Dict)):
            choices = node.args[0]
            if not all(isinstance(key, ast.Constant) and isinstance(key.value, str)
                       for key in choices.keys):
                self.skipped['unknown_select_keys'] += 1
                return original
            keys = {key.value for key in choices.keys}
            cpu_only = keys <= CPU_KEYS | {'//conditions:default'}
            replacements = []
            for key, val in zip(choices.keys, choices.values):
                selected = arches
                if cpu_only:
                    selected = tuple(cpu for cpu in arches
                                     if key.value == '@platforms//cpu:' + cpu
                                     or key.value == '//conditions:default'
                                     and '@platforms//cpu:' + cpu not in keys)
                updated = self.expression(val, target, selected, field)
                if updated != self.source.text(val):
                    replacements.append((*self.source.span(val), updated))
            if not replacements:
                return original
            start, end = self.source.span(node)
            # Preserve the condition keys and select() even if all its
            # branches become empty; a missing match must still be an error.
            rewritten = self.source.rewrite(replacements).encode('utf-8')
            added = sum(len(value.encode('utf-8')) - (last - first)
                        for first, last, value in replacements)
            return rewritten[start:end + added].decode('utf-8')
        if (function != 'glob' or len(node.args) != 1 or self.doc.globs is None
                or '#' in original):
            self.skipped['unknown_calls_or_globs'] += 1
            return original
        opts = {kw.arg: kw.value for kw in node.keywords}
        patterns = _literal_strings(node.args[0])
        excludes = _literal_strings(opts['exclude']) if 'exclude' in opts else []
        if patterns is None or excludes is None or set(opts) - {'exclude', 'allow_empty'}:
            self.skipped['unknown_glob_options'] += 1
            return original
        values = self.doc.globs.result(patterns, excludes)
        if self.covered(values, target, arches):
            self.remember(node, target, field, 'whole_globs', values, arches)
            return '[]'
        retained = []
        for pattern in patterns:
            matching = self.doc.globs.result([pattern], excludes)
            if self.covered(matching, target, arches):
                self.remember(node, target, field, 'glob_patterns', matching, arches)
            else:
                retained.append(pattern)
                partial = set(label(value, self.doc.repo) for value in matching)
                inherited = set.intersection(*(set(self.graph.inherited(target, cpu)) for cpu in arches))
                overlap = partial & inherited
                if overlap:
                    self.skipped['partial_glob_patterns'] += 1
                    self.partials.append({
                        'target': target, 'field': field, 'line': node.lineno,
                        'pattern': pattern, 'headers': len(partial), 'inherited': len(overlap),
                    })
        if len(retained) == len(patterns):
            return original
        # Preserve every existing glob option after deleting complete covered
        # patterns; never replace a partial pattern with hundreds of literals.
        parts = [json.dumps(retained)]
        parts.extend(kw.arg + ' = ' + self.source.text(kw.value) for kw in node.keywords)
        return 'glob(' + ', '.join(parts) + ')'

    def text(self):
        edits = []
        for _, call in self.source.rules():
            attrs = {kw.arg: kw.value for kw in call.keywords}
            name = attrs.get('name')
            if (getattr(call.func, 'id', '') not in CC_COMPILERS
                    or not isinstance(name, ast.Constant) or not isinstance(name.value, str)):
                continue
            if 'include_prefix' in attrs or 'strip_include_prefix' in attrs:
                self.skipped['virtual_header_rules'] += 1
                continue
            target = label(':' + name.value, self.doc.repo)
            for field in PUBLIC_HEADERS:
                value = attrs.get(field)
                if value is None:
                    continue
                updated = self.expression(value, target, ARCHES, field)
                if updated != self.source.text(value):
                    edits.append((*self.source.span(value), updated))
        return self.source.rewrite(edits)


def prune(root, facts, *, write=False, report=None):
    inventories = inventories_at_pins(facts)
    # Root libraries may connect two external public deps, but only pinned
    # external sources may prove coverage or be edited.
    paths = [root / 'BUILD.bazel', *sorted((root / 'bazel/dependencies').glob('*.BUILD.bazel'))]
    docs = [Document(path, inventories) for path in paths]
    graph = PublicGraph(docs)
    total, skipped, files, details, partials, updates = Counter(), Counter(), {}, [], [], {}
    after_docs = []
    for doc in docs:
        if doc.repo == '@clickhouse' or doc.files is None:
            after_docs.append(doc)
            if doc.repo != '@clickhouse':
                skipped['unverified_repositories'] += 1
            continue
        rewrite = Rewrite(doc, graph)
        after = rewrite.text()
        total.update(rewrite.counts)
        skipped.update(rewrite.skipped)
        details.extend(rewrite.details)
        partials.extend(rewrite.partials)
        if after == doc.source.bytes.decode():
            after_docs.append(doc)
            continue
        changed = Document(doc.path, inventories, text=after)
        if non_header_statements(doc) != non_header_statements(changed):
            raise ValueError('Changed a non-header BUILD statement: ' + str(doc.path))
        after_docs.append(changed)
        updates[doc.path] = (doc.source.bytes.decode(), after)
        files[doc.path.name] = dict(rewrite.counts)
    after_graph = PublicGraph(after_docs)
    if graph.rules.keys() != after_graph.rules.keys():
        raise ValueError('Rule names changed')
    verified, digest = Counter(), hashlib.sha256()
    for target, (_, kind, _) in graph.rules.items():
        if kind not in CC_PROVIDERS | {'alias'}:
            continue
        for cpu in ARCHES:
            before = graph.own_headers(target, cpu) | graph.inherited(target, cpu)
            after = after_graph.own_headers(target, cpu) | after_graph.inherited(target, cpu)
            if before != after:
                raise ValueError(f'{target} ({cpu}): public headers changed: '
                                 f'lost={sorted(before - after)[:30]} gained={sorted(after - before)[:30]}')
            digest.update((target + '\0' + cpu + '\0').encode())
            for header in sorted(before):
                digest.update((header + '\0').encode())
            verified['rule_cpu_pairs'] += 1
            verified['header_rule_cpu_pairs'] += len(before)
    result = {'files': files, 'counts': dict(total), 'skipped': dict(skipped),
              'verified': dict(verified), 'public_headers_sha256': digest.hexdigest(),
              'inventory_repos': len(inventories), 'details': details, 'partial_globs': partials}
    if report:
        report.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    if write:
        # All checks precede the first write, including detecting concurrent
        # BUILD changes so no newer edit is silently replaced.
        for path, (before, _) in updates.items():
            if path.read_text() != before:
                raise ValueError('BUILD changed during audit: ' + str(path))
        for path, (_, after) in updates.items():
            path.write_text(after)
    summary = {key: value for key, value in result.items() if key not in {'details', 'partial_globs'}}
    summary['changed_files'] = len(files)
    summary['written'] = write
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--facts', required=True, type=Path)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--report', type=Path, help='Detailed removals and skipped partial globs')
    args = parser.parse_args()
    print(json.dumps(prune(args.root, args.facts, write=args.write, report=args.report),
                     indent=2, sort_keys=True))


if __name__ == '__main__':
    main()

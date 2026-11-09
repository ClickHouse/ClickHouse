#!/usr/bin/env python3
"""Remove generated translation units accidentally published as hdrs.

Every removed file is generated or configured, already compiled once, and has
no possible recorded include or forced include. Complete SHA-checked source
receipts are required. Private textual C/C++ includes and all generation rules
are left alone. The default is a read-only preview.
"""

import argparse
import ast
from collections import Counter, defaultdict
from functools import cache
import json
from pathlib import Path, PurePosixPath
import posixpath
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.audit_local_compiler_inputs import LocalAudit, known_expression
from initial_migration.native_cc_dependencies import DependencyGraph
from initial_migration.native_cc_inputs import FORCED, SPECIAL_REPOS, Sources
from initial_migration.native_textual_inputs import expanded
from initial_migration.prune_inherited_headers import (
    ARCHES, CC_COMPILERS, Document, Rewrite, label,
)
from initial_migration.repository_inputs import ROOT

CPP_TUS = ('.c', '.cc', '.cpp', '.cxx', '.c++', '.C')


class TranslationUnits:
    def __init__(self, graph, sources):
        self.graph, self.sources = graph, sources
        self.compilers = defaultdict(list)
        self.inputs = LocalAudit(graph, sources)
        self.incoming = defaultdict(list)
        self.forced = defaultdict(list)
        self.decisions = Counter()
        self.evidence = {}
        for target, (doc, kind, attrs) in graph.rules.items():
            if kind not in CC_COMPILERS or doc.repo in SPECIAL_REPOS:
                continue
            if not known_expression(doc, attrs.get('srcs'), 'srcs'):
                continue
            for cpu in ARCHES:
                for value in doc.values(attrs.get('srcs'), cpu):
                    source = label(value, doc.repo)
                    if source.endswith(CPP_TUS):
                        self.compilers[source, cpu].append(target)
                opts = doc.values(attrs.get('copts'), cpu)
                for pos, option in enumerate(opts):
                    if option in FORCED and pos + 1 < len(opts):
                        operand = opts[pos + 1]
                    else:
                        prefix = next((x for x in FORCED if option.startswith(x + '=')), None)
                        if prefix is None:
                            continue
                        operand = option[len(prefix) + 1:]
                    self.forced[PurePosixPath(operand.rstrip(')')).name].append((target, cpu, option))

        # Check complete directives from every scanned source, not just the
        # owner's sources. A quote include can use any original include root.
        for source, rows in sources.directives.items():
            for line, directive, syntax, operand in rows:
                if syntax == 'computed':
                    continue
                name = PurePosixPath(posixpath.normpath(operand)).name
                self.incoming[name].append((source, line, directive, syntax, operand))

    @cache
    def safe(self, candidate, cpu):
        owners = self.compilers.get((candidate, cpu), ())
        if len(owners) != 1:
            return 'not_compiled_exactly_once'
        owner = owners[0]
        basename = PurePosixPath(candidate.rsplit(':', 1)[-1]).name
        if candidate not in self.sources.directives:
            return 'generated_source_not_scanned'
        if candidate in self.sources.inventory:
            return 'not_generated_or_configured'
        if self.incoming.get(basename):
            self.evidence[candidate, cpu] = self.incoming[basename]
            return 'possible_include'
        if self.forced.get(basename):
            self.evidence[candidate, cpu] = self.forced[basename]
            return 'forced_input'

        # A computed include may select a source file that has no recorded
        # literal include. Do not infer its value from the source extension.
        for public in (False, True):
            incoming, computed, generated, _, unresolved = self.inputs.reachable(owner, cpu, public)
            if candidate in incoming:
                self.evidence[candidate, cpu] = [incoming[candidate]]
                return 'owner_textual_include'
            if generated:
                return 'unknown_owner_generated_sources'
            if computed or unresolved:
                self.evidence[candidate, cpu] = computed
                return 'unresolved_owner_include'
        return 'unpublished_generated_tu'


class RemovePublishedTUs(Rewrite):
    def __init__(self, doc, graph, audit):
        super().__init__(doc, graph)
        self.audit = audit

    def covered(self, value, target, arches, field):
        candidate = label(value, self.doc.repo)
        if (field != 'hdrs' or not arches or not candidate.endswith(CPP_TUS)
                or target not in self.graph.rules or candidate in self.graph.rules):
            return False
        remove = True
        for cpu in arches:
            reason = self.audit.safe(candidate, cpu)
            self.audit.decisions[reason] += 1
            if reason != 'unpublished_generated_tu':
                remove = False
        return remove


def verify(before, after):
    """Freeze every rule and ordered source except the selected hdrs literals."""
    old = list(before.source.rules())
    new = list(after.source.rules())
    if len(old) != len(new):
        raise ValueError(str(before.path) + ': rule count changed')
    counts = Counter()
    for (_, was), (_, now) in zip(old, new, strict=True):
        old_fields = {k.arg: k.value for k in was.keywords}
        new_fields = {k.arg: k.value for k in now.keywords}
        kind = getattr(was.func, 'id', '')
        if kind != getattr(now.func, 'id', '') or old_fields.keys() != new_fields.keys():
            raise ValueError(str(before.path) + ': changed rule schema')
        name = ast.literal_eval(old_fields['name'])
        for field, node in old_fields.items():
            if kind in CC_COMPILERS and field == 'hdrs':
                continue
            if expanded(node, before) != expanded(new_fields[field], after):
                raise ValueError(f'{before.path}:{name}: changed {field}')
        if kind in CC_COMPILERS:
            for cpu in ARCHES:
                if before.values(old_fields.get('srcs'), cpu) != after.values(new_fields.get('srcs'), cpu):
                    raise ValueError(f'{before.path}:{name}: changed {cpu} ordered srcs')
                before_headers = before.labels(old_fields.get('hdrs'), cpu)
                after_headers = after.labels(new_fields.get('hdrs'), cpu)
                if after_headers - before_headers:
                    raise ValueError(f'{before.path}:{name}: added {cpu} headers')
                counts['removed_tus'] += len(before_headers - after_headers)
                counts['ordered_sources'] += 1
        counts['rules'] += 1
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--receipts', type=Path, required=True)
    parser.add_argument('--report', type=Path)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    sources = Sources(args.receipts, args.root)
    sources.require_complete()
    docs = [Document(path, sources.inventories) for path in [
        args.root / 'BUILD.bazel', *sorted((args.root / 'bazel/dependencies').glob('*.BUILD.bazel'))]]
    graph = DependencyGraph(docs, sources)
    audit = TranslationUnits(graph, sources)
    stats = Counter()
    edits = []
    for doc in docs:
        if doc.repo in SPECIAL_REPOS:
            continue
        rewrite = RemovePublishedTUs(doc, graph, audit)
        result = rewrite.text()
        if result == doc.source.bytes.decode():
            continue
        after = Document(doc.path, sources.inventories, result)
        stats.update(verify(doc, after))
        stats['files'] += 1
        stats['hdrs_literals'] += rewrite.stats['inherited_source_header_literals']
        edits.append((doc, result))
        print(doc.path.relative_to(args.root), rewrite.stats['inherited_source_header_literals'])
    if args.write:
        for doc, _ in edits:
            if doc.path.read_bytes() != doc.source.bytes:
                raise ValueError('BUILD changed during audit: ' + str(doc.path))
        for doc, result in edits:
            doc.path.write_text(result)
    print('Generated TU header audit:', dict(stats), 'written' if args.write else 'no files written')
    print('Decisions:', dict(audit.decisions))
    if args.report:
        args.report.write_text(json.dumps({
            'changes': dict(stats), 'decisions': dict(audit.decisions),
            'retained': {f'{name} [{cpu}]': rows for (name, cpu), rows in audit.evidence.items()},
        }, indent=2) + '\n')


if __name__ == '__main__':
    main()

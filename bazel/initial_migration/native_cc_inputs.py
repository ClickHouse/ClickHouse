#!/usr/bin/env python3
"""Find native textual inputs by traversing includes from Bazel sources.

The receipt files contain the complete include directives and pinned inventory
of each external repository, after its BUILD overlays. This initial migration
pass edits compiler inputs only; genrule and expand_template inputs are never
considered native compilation inputs.
"""

import argparse
import ast
from collections import Counter, defaultdict, deque
import gzip
import json
from pathlib import Path, PurePosixPath
import posixpath
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.prune_inherited_headers import (
    ARCHES, CC_COMPILERS, Document, Graph, Rewrite, label,
)
from initial_migration.repository_inputs import CC_HEADER_SUFFIXES, REPOSITORIES, ROOT

TUS = ('.c', '.cc', '.cpp', '.cxx', '.c++', '.C', '.s', '.S', '.asm', '.ASM')
# Preserve textual source families which are also selected by C/C++ macros.
# Templates and schemas belong to codegen, unless a native source includes one.
TEXTUAL = (*CC_HEADER_SUFFIXES, '.def', '.defs', '.th', '.td', '.tbl', '.tbi',
           '.tpp', '.icc', '.ixx', '.cuh', '.msh')
FORCED = {'-include', '-imacros', '-ivfsoverlay', '-fmodule-file', '-fmodule-map-file'}
SPECIAL_REPOS = {'@clickhouse_sysroot', '@clickhouse_llvm_project',
                 '@clickhouse_rust_vendor', '@clickhouse_delta_kernel_rs'}


class Sources:
    def __init__(self, receipt_dir, root):
        self.directives = {}
        self.inventories = {}
        self.inventory = set()
        self.by_name = defaultdict(set)
        self.scanned = set()
        self.root = root
        paths = sorted(receipt_dir.glob('*-v[23]-*.json.gz'))
        newest = {}
        for path in paths:
            with gzip.open(path, 'rt') as file:
                data = json.load(file)
            owner = data['owner']
            if owner in newest and newest[owner]['version'] >= data['version']:
                continue
            newest[owner] = data
        for owner, data in newest.items():
            if owner in REPOSITORIES:
                spec = REPOSITORIES[owner]
                if spec['sha256'] != data['sha256'] or spec['commit'] != data['commit']:
                    raise ValueError('Receipt does not match configured source pin: ' + owner)
                repo = '@' + spec['name']
                if repo in SPECIAL_REPOS:
                    continue
                self.inventories[owner] = data['inventory']
            else:
                if owner or data['name'] or data['sha256'] is not None:
                    raise ValueError('Unknown receipt owner: ' + owner)
                changed = subprocess.check_output(
                    ['git', 'diff', '--name-only', '-z', data['commit'], '--'], cwd=root,
                ).decode().split('\0')
                if set(changed).intersection(data['verified_blobs']):
                    raise ValueError('Root source files changed since the include scan; collect a new receipt')
                repo = '@clickhouse'
            self.scanned.add(repo)
            prefix = repo + '//:'
            for name in data['inventory']:
                key = prefix + name
                self.inventory.add(key)
                self.by_name[PurePosixPath(name).name].add(key)
            for name, rows in data['files'].items():
                self.directives[prefix + name] = rows
            for link, destination in data.get('source_links', {}).items():
                if prefix + destination in self.directives:
                    self.directives[prefix + link] = self.directives[prefix + destination]

    def include_candidates(self, source, operand, syntax):
        """Superset of source files that a physical include can resolve to.

        Keep all matching repositories where the search roots are unknown.
        Never infer use from the mere existence of another includer in an
        archive. Only called while traversing a reachable source/header.
        """
        repo, sep, filename = source.partition('//:')
        if not sep or not operand or operand.startswith(('/', '$(')):
            return ()
        if syntax in {'quote', 'apostrophe'}:
            relative = repo + '//:' + posixpath.normpath(posixpath.join(
                posixpath.dirname(filename), operand))
            if relative in self.inventory:
                return (relative,)
        requested = posixpath.normpath(operand)
        # The original compiler roots may point anywhere within a pinned
        # repository. Resolve the complete relative include, not its basename.
        if requested.startswith('../'):
            while requested.startswith('../'):
                requested = requested.removeprefix('../')
        candidates = self.by_name.get(posixpath.basename(requested), ())
        return tuple(candidate for candidate in candidates
                     if (name := candidate.partition('//:')[2]) == requested
                     or name.endswith('/' + requested))


def initial_sources(graph, sources):
    roots = set()
    targets = set()
    for target, (doc, kind, attrs) in graph.rules.items():
        if kind not in CC_COMPILERS:
            continue
        for arch in ARCHES:
            srcs = doc.labels(attrs.get('srcs'), arch)
            roots.update(source for source in srcs if source.endswith(TUS))
            # An owner's hdrs and textual_hdrs are the public compilation API.
            # Copied foreign files are not evidence that a library uses them.
            prefix = doc.repo + '//:'
            for field in ('hdrs', 'textual_hdrs'):
                roots.update(source for source in doc.labels(attrs.get(field), arch)
                             if source.startswith(prefix))
            copts = doc.values(attrs.get('copts'), arch)
            for i, option in enumerate(copts):
                if option in FORCED and i + 1 < len(copts):
                    raw = copts[i + 1]
                    targets.update(sources.include_candidates(target, raw, 'quote'))
    return (roots | targets) & sources.inventory


def include_closure(graph, sources):
    pending = deque(sorted(initial_sources(graph, sources)))
    reached, computed = set(), Counter()
    while pending:
        file = pending.popleft()
        if file in reached:
            continue
        reached.add(file)
        for line, directive, syntax, operand in sources.directives.get(file, ()):
            if syntax == 'computed':
                computed[file.partition('//:')[0]] += 1
                continue
            pending.extend(candidate for candidate in sources.include_candidates(file, operand, syntax)
                           if candidate not in reached)
    return reached, computed


class NativeInputs(Rewrite):
    def __init__(self, doc, graph, sources, live):
        super().__init__(doc, graph)
        self.sources, self.live = sources, live
        self.kept = Counter()

    def covered(self, value, target, arches, field):
        if field != 'additional_compiler_inputs' or not arches or target not in self.graph.rules:
            return False
        candidate = label(value, self.doc.repo)
        repo, _, filename = candidate.partition('//:')
        if (repo in SPECIAL_REPOS or repo == '@clickhouse' or repo not in self.sources.scanned
                or candidate not in self.sources.inventory or candidate in self.graph.rules):
            self.kept['generated_rule_or_unscanned'] += 1
            return False
        if filename.endswith(TEXTUAL):
            self.kept['c_cpp_textual_family'] += 1
            return False
        if candidate in self.live:
            self.kept['reachable_from_native_sources_or_public_headers'] += 1
            return False
        self.stats['unused_native_data'] += 1
        return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--receipts', required=True, type=Path)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    sources = Sources(args.receipts, args.root)
    documents = [Document(path, sources.inventories) for path in [
        args.root / 'BUILD.bazel', *sorted((args.root / 'bazel/dependencies').glob('*.BUILD.bazel'))]]
    graph = Graph(documents)
    live, computed = include_closure(graph, sources)
    total, owners, held = Counter(), Counter(), Counter()
    for doc in documents:
        rewrite = NativeInputs(doc, graph, sources, live)
        after = rewrite.text()
        held.update(rewrite.kept)
        if after != doc.source.bytes.decode():
            ast.parse(after, filename=str(doc.path))
            total.update(rewrite.stats)
            owners.update(rewrite.removed_by_owner)
            total['files'] += 1
            print(doc.path.relative_to(args.root), dict(rewrite.stats))
            if args.write:
                doc.path.write_text(after)
    print('Scanned source repositories:', len(sources.scanned), 'live sources:', len(live),
          'literal include sources:', len(sources.directives),
          'reachable computed directives:', sum(computed.values()))
    print('Results:', dict(total), 'written' if args.write else 'no files written')
    print('Removed literals by owner:', owners.most_common())
    print('Remaining:', dict(held))


if __name__ == '__main__':
    main()

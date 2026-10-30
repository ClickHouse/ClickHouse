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
from functools import lru_cache
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
SPECIAL_REPOS = {'@clickhouse_llvm_project',
                 '@clickhouse_rust_vendor', '@clickhouse_delta_kernel_rs'}


class Sources:
    def __init__(self, receipt_dir, root):
        self.directives = {}
        self.inventories = {}
        self.inventory = set()
        self.by_name = defaultdict(set)
        self.scanned = set()
        self.complete = set()
        self.source_copies = {}
        self.root = root
        paths = sorted(receipt_dir.glob('*-v[23]-*.json.gz')) if receipt_dir is not None else []
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
                # Document.owner is None for the main repository. These are
                # real header and source globs, including compatibility
                # wrappers in contrib/*-cmake.
                self.inventories[None] = data['inventory']
            self.scanned.add(repo)
            if data['version'] >= 3:
                self.complete.add(repo)
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
        self.known_generated_sources()

    def known_generated_sources(self):
        """Account for symlinked sources, rendered arrays and checked-in configs."""
        from initial_migration.scan_include_adjacency import includes
        codegen = self.root / 'bazel/rules/codegen_rules.bzl'
        tree = ast.parse(codegen.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            attrs = {k.value: v for k, v in zip(node.keys, node.values)
                     if isinstance(k, ast.Constant) and isinstance(k.value, str)}
            kind = attrs.get('kind')
            if not isinstance(kind, ast.Constant):
                continue
            if kind.value == 'licenses':
                # generate_licenses.py renders license contents inside raw
                # strings in a const char* array; no include directives.
                for out in ast.literal_eval(attrs['outs']):
                    self.directives['@clickhouse//bazel:' + out] = []
            elif kind.value == 'embedded_sources':
                # _embedded_sources_impl symlinks each output to its source.
                for src, out in zip(ast.literal_eval(attrs['srcs']),
                                    ast.literal_eval(attrs['outs']), strict=True):
                    original = label(src, '@clickhouse')
                    if original not in self.directives:
                        continue
                    for arch in ARCHES:
                        dest = '@clickhouse//bazel:' + out.replace('{arch}', arch)
                        self.directives[dest] = self.directives[original]
                        self.source_copies[dest] = original

        tree = ast.parse((self.root / 'bazel/rules/build_options_data.bzl').read_text())
        data = next(stmt.value for stmt in tree.body if isinstance(stmt, ast.Assign) and
                    any(isinstance(t, ast.Name) and t.id == 'BUILD_OPTIONS' for t in stmt.targets))
        build = ast.parse((self.root / 'bazel/BUILD.bazel').read_text())
        configured = [
            {kw.arg: kw.value for kw in stmt.value.keywords}
            for stmt in build.body
            if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call)
            and getattr(stmt.value.func, 'id', '') == 'build_options'
        ]
        directives = defaultdict(list)
        for node in data.values:
            attrs = {k.value: v for k, v in zip(node.keys, node.values)}
            template = ast.literal_eval(attrs['template'])
            # Original CMake capture is still CPU-specific. After
            # configured_native_codegen the rule owns the single output.
            if 'out' in attrs:
                out = ast.literal_eval(attrs['out'])
            elif len(configured) == 1:
                out = ast.literal_eval(configured[0]['out'])
            else:
                raise ValueError('Expected one configured build_options output')
            # build_options.bzl only substitutes quoted flag/version strings
            # and stringification defines. Includes come from the template.
            directives[out].append(includes((self.root / 'bazel' / template).read_bytes(), template))
        for out, templates in directives.items():
            # Never claim a CPU-independent include body if either CPU's
            # template cannot be scanned or its directives actually differ.
            if templates[0] is not None and all(value == templates[0] for value in templates):
                self.directives['@clickhouse//bazel:' + out] = templates[0]

        for path in (self.root / 'bazel/config').rglob('*'):
            if path.is_file() and path.name.endswith(TUS):
                rel = path.relative_to(self.root / 'bazel').as_posix()
                self.directives['@clickhouse//bazel:' + rel] = includes(path.read_bytes(), rel)

    def register_templates(self, graph):
        """Model expand_template when replacements cannot create directives."""
        for target, (doc, kind, attrs) in graph.rules.items():
            if kind != 'expand_template':
                continue
            try:
                substitutions = ast.literal_eval(attrs['substitutions'])
                template = label(ast.literal_eval(attrs['template']), doc.repo)
                out = label(ast.literal_eval(attrs['out']), doc.repo)
            except (KeyError, ValueError, TypeError):
                continue
            if (template not in self.directives or not isinstance(substitutions, dict) or
                    any(not isinstance(key, str) or not isinstance(value, str) or
                        any(char in value for char in '\n\r#\\')
                        for key, value in substitutions.items())):
                continue
            rows = []
            for line, directive, syntax, operand in self.directives[template]:
                for key, value in substitutions.items():
                    operand = operand.replace(key, value)
                rows.append((line, directive, syntax, operand))
            self.directives[out] = rows

    def require_complete(self):
        expected = ({'@' + spec['name'] for spec in REPOSITORIES.values()} |
                    {'@clickhouse'}) - SPECIAL_REPOS
        missing = expected - self.complete
        if missing:
            raise ValueError('Collect complete v3 include receipts before editing: ' +
                             ', '.join(sorted(missing)))

    @lru_cache(maxsize=262144)
    def include_candidates(self, source, operand, syntax):
        """Superset of source files that a physical include can resolve to.

        Keep all matching repositories where the search roots are unknown.
        Never infer use from the mere existence of another includer in an
        archive. Only called while traversing a reachable source/header.
        """
        source = self.source_copies.get(source, source)
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
    if args.write:
        sources.require_complete()
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

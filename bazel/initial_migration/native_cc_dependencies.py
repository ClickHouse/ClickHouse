#!/usr/bin/env python3
"""Replace captured foreign source headers with their CcInfo dependencies.

Only includes reached from the target's actual translation units or owned
public headers justify new dependencies. Current raw hdrs from foreign
repositories are not public API declarations of this target.
"""
import argparse
import ast
from collections import Counter, defaultdict, deque
from functools import cache, lru_cache
import json
from pathlib import Path
import posixpath
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.native_cc_inputs import FORCED, SPECIAL_REPOS, Sources, TUS
from initial_migration.native_public_includes import directory, list_elements
from initial_migration.prune_inherited_headers import (
    ARCHES, CC_COMPILERS, CC_PROVIDERS, Document, Graph, Rewrite, label,
)
from initial_migration.repository_inputs import CC_HEADER_SUFFIXES, ROOT


class DependencyGraph(Graph):
    def __init__(self, documents, sources):
        super().__init__(documents)
        self.sources = sources
        self.sources.register_templates(self)
        self.public = defaultdict(set)
        self.private = defaultdict(set)
        self.published = defaultdict(list)
        self.available = {}
        self.requests = Counter()
        self.unresolved = Counter()
        for target, (doc, kind, attrs) in self.rules.items():
            if kind not in CC_PROVIDERS:
                continue
            for cpu in ARCHES:
                for header in self.own_headers(target, cpu):
                    self.published[header, cpu].append(target)

    def private_dependencies(self, target, cpu):
        if target not in self.rules:
            return set()
        doc, kind, attrs = self.rules[target]
        return (doc.labels(attrs.get('implementation_deps'), cpu) |
                self.private[target, cpu])

    def visible(self, target, cpu, public_only=False):
        pending = list(self.dependencies(target, cpu)) + list(self.public[target, cpu])
        if not public_only:
            pending += list(self.private_dependencies(target, cpu))
        headers, seen = set(), {target}
        while pending:
            dep = pending.pop()
            if dep in seen:
                continue
            seen.add(dep)
            headers.update(self.own_headers(dep, cpu))
            if self.rules.get(dep, (None, None, None))[1] in CC_PROVIDERS | {'alias'}:
                pending.extend(self.dependencies(dep, cpu))
                pending.extend(self.public[dep, cpu])
        return headers

    @cache
    def archive_sources(self, target, cpu):
        doc, kind, attrs = self.rules[target]
        return tuple(value for value in doc.labels(attrs.get('srcs'), cpu) if value.endswith(TUS))

    @cache
    def direct_graph_inputs(self, target, cpu):
        """Include generated-rule dependencies when detecting Bazel cycles."""
        if target not in self.rules:
            return frozenset()
        doc, kind, attrs = self.rules[target]
        deps = set()
        for field, value in attrs.items():
            if field not in {'name', 'out', 'outs', 'copts', 'local_includes', 'includes',
                             'defines', 'local_defines', 'features', 'visibility', 'tags', 'cmd'}:
                deps.update(doc.labels(value, cpu))
        return frozenset(ref for ref in deps if ref in self.rules)

    def cyclic(self, consumer, provider, cpu):
        pending, seen = [provider], set()
        while pending:
            item = pending.pop()
            if item == consumer:
                return True
            if item in seen:
                continue
            seen.add(item)
            pending.extend(self.direct_graph_inputs(item, cpu))
            pending.extend(self.public[item, cpu])
            pending.extend(self.private[item, cpu])
        return False

    def compile_files(self, target, cpu):
        key = target, cpu
        if key not in self.available:
            doc, kind, attrs = self.rules[target]
            result = self.visible(target, cpu)
            for field in ('srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs'):
                result.update(doc.labels(attrs.get(field), cpu))
            self.available[key] = frozenset(result)
        return self.available[key]

    @cache
    def original_include_roots(self, target, cpu):
        """Physical source directories searched by the original compile."""
        pending = [(target, True)]
        seen, roots = set(), set()
        while pending:
            ref, direct = pending.pop()
            if ref in seen or ref not in self.rules:
                continue
            seen.add(ref)
            doc, kind, attrs = self.rules[ref]
            if kind not in CC_COMPILERS | {'alias'}:
                continue
            for field in (('local_includes', 'includes', 'copts') if direct else ('includes',)):
                nodes = list_elements(attrs.get(field), doc, cpu)
                index = 0
                while index < len(nodes):
                    node = nodes[index]
                    value = directory(node, doc)
                    index += 1
                    if value is None:
                        continue
                    if field == 'copts':
                        if value in {'-I', '-isystem', '-iquote'} and index < len(nodes):
                            node = nodes[index]
                            value = directory(node, doc)
                            index += 1
                        elif value.startswith('-I') and len(value) > 2:
                            value = value[2:]
                        elif value.startswith('-isystem') and len(value) > len('-isystem'):
                            value = value[len('-isystem'):].strip()
                        else:
                            continue
                    if not value or value.startswith(('$(', '-', '/')):
                        continue
                    contains_repo = any(isinstance(part, ast.Call) and
                        getattr(part.func, 'id', '') in {'repository_path', 'repository_local_include'}
                        for part in ast.walk(node))
                    absolute = value if contains_repo or not doc.owner else posixpath.join(doc.owner, value)
                    roots.add(posixpath.normpath(absolute))
            pending.extend((dep, False) for dep in self.dependencies(ref, cpu))
            if direct:
                pending.extend((dep, False) for dep in self.private_dependencies(ref, cpu))
        return frozenset(roots)

    @lru_cache(maxsize=524288)
    def under_original_include(self, target, cpu, candidate, operand):
        repo, sep, filename = candidate.partition('//:')
        doc = self.documents.get(repo)
        if not sep or doc is None:
            return False
        physical = posixpath.join(doc.owner or '', filename)
        normal = posixpath.normpath(operand)
        if not normal.startswith('../'):
            if physical == normal:
                return '.' in self.original_include_roots(target, cpu)
            suffix = '/' + normal
            return (physical.endswith(suffix) and
                    physical.removesuffix(suffix) in self.original_include_roots(target, cpu))
        return any(posixpath.normpath(posixpath.join(root, operand)) == physical
                   for root in self.original_include_roots(target, cpu))

    @cache
    def repository_compile_files(self, repo, cpu):
        headers = set()
        for target, (doc, kind, attrs) in self.rules.items():
            if doc.repo == repo and kind in CC_COMPILERS and self.archive_sources(target, cpu):
                headers.update(self.compile_files(target, cpu))
        return frozenset(headers)

    def used(self, target, cpu, public_only):
        doc, kind, attrs = self.rules[target]
        if public_only:
            roots = set(self.own_headers(target, cpu))
        else:
            roots = set(self.archive_sources(target, cpu))
            opts = doc.values(attrs.get('copts'), cpu)
            for i, value in enumerate(opts[:-1]):
                if value in FORCED:
                    roots.update(self.sources.include_candidates(target, opts[i + 1], 'quote'))
        pending, live, computed = deque(roots), set(), set()
        available = self.compile_files(target, cpu)
        if not self.archive_sources(target, cpu):
            # Only names provided to an actual compilation in this repository
            # justify a missing dependency of a header-only target. Resolving
            # an unavailable basename across every archive conflates unrelated
            # platform headers such as config.h, string.h and client.h.
            available = available | self.repository_compile_files(doc.repo, cpu)
        while pending:
            path = pending.popleft()
            if path in live:
                continue
            live.add(path)
            for line, directive, syntax, operand in self.sources.directives.get(path, ()):
                if syntax == 'computed':
                    computed.add(path.partition('//:')[0])
                    continue
                candidates = self.sources.include_candidates(path, operand, syntax)
                in_scope = set(candidates).intersection(available)
                # A previous pruning pass can have deleted a required raw
                # header. Its original include directory still proves the
                # exact file the compiler needs, even before deps are repaired.
                in_scope.update(ref for ref in candidates
                                if self.under_original_include(target, cpu, ref, operand))
                pending.extend(ref for ref in in_scope if ref not in live)
        return frozenset(live), frozenset(computed)

    def provider(self, consumer, header, cpu):
        candidates = self.published.get((header, cpu), ())
        ranked = sorted(candidates, key=lambda target: (
            bool(self.archive_sources(target, cpu)),
            '__compile_' in target,
            len(self.own_headers(target, cpu)),
            target,
        ))
        for choice in ranked:
            owner, _, attrs = self.rules[choice]
            consumer_doc = self.rules[consumer][0]
            if owner.repo == '@clickhouse' and consumer_doc.repo != owner.repo:
                # A migrated ClickHouse target can carry all captured headers.
                # It does not own a dependency's config because its broad hdrs
                # glob happens to contain contrib/<dependency>-cmake.
                source_files = owner.labels(attrs.get('srcs'), cpu)
                if source_files:
                    prefix = owner.repo + '//:'
                    name = header.removeprefix(prefix)
                    component = '/'.join(name.split('/')[:2]) + '/'
                    if not any(ref.startswith(prefix + component) and ref.endswith(TUS)
                               for ref in source_files):
                        continue
            if choice != consumer and not self.cyclic(consumer, choice, cpu):
                return choice
        return None

    def prepare(self, consumers, compilation_only=False):
        """Add only dependencies supported by source/public include directives."""
        evidence = {}
        for target, (doc, kind, attrs) in self.rules.items():
            if kind not in CC_COMPILERS - {'cc_import'} or doc.repo not in consumers:
                continue
            for cpu in ARCHES:
                # Even when repairing only compilation deps, public headers
                # are evidence for retaining another public header. A source
                # may not use its config while a downstream public API does.
                pub, pub_computed = self.used(target, cpu, True)
                private, priv_computed = self.used(target, cpu, False)
                # Generated translation units cannot be inspected from pinned
                # sources. Their headers can still be removed if the exact
                # same file arrives through a CcInfo dependency.
                srcs = doc.labels(attrs.get('srcs'), cpu)
                generated = any(ref in self.rules or (ref.endswith(TUS) and
                                ref not in self.sources.directives) for ref in srcs)
                evidence[target, cpu] = (pub, private, pub_computed | priv_computed, generated)
        # Freeze original availability and evidence before adding any deps.
        # New dependencies cannot justify other new dependencies.
        for (target, cpu), (pub, private, _, _) in evidence.items():
            doc, kind, attrs = self.rules[target]
            visible_public = self.visible(target, cpu, public_only=True)
            visible_compile = self.visible(target, cpu)
            # Public headers require public dependencies. Sources require
            # private compilation deps except for rules_cc binaries/tests.
            requested = ((False, private),) if compilation_only else (
                (True, pub), (False, private - pub),
            )
            for public, headers in requested:
                for file in sorted(headers):
                    if file.partition('//:')[0] == doc.repo or file not in self.sources.inventory:
                        continue
                    visible = visible_public if public else visible_compile
                    if file in visible:
                        continue
                    provider = self.provider(target, file, cpu)
                    if provider is None:
                        self.requests['no_acyclic_provider'] += 1
                        self.unresolved[target, file, cpu] += 1
                        continue
                    field = self.public if public or kind in {'cc_binary', 'cc_test'} else self.private
                    field[target, cpu].add(provider)
                    self.requests['public_deps' if field is self.public else 'implementation_deps'] += 1
                    visible.update(self.visible(provider, cpu, public_only=True))
                    visible.update(self.own_headers(provider, cpu))
                    if public:
                        visible_compile.update(visible)
        return evidence


class HeaderRewrite(Rewrite):
    def __init__(self, doc, graph, evidence):
        super().__init__(doc, graph)
        self.evidence = evidence
        self.published_cache = {}

    def covered(self, value, target, arches, field):
        if not arches or (target, arches[0]) not in self.evidence:
            return False
        candidate = label(value, self.doc.repo)
        repo, sep, filename = candidate.partition('//:')
        if (not sep or repo == self.doc.repo or candidate in self.graph.rules or
                candidate not in self.graph.sources.inventory or
                not filename.endswith((*CC_HEADER_SUFFIXES, '.def', '.defs', '.th'))):
            return False
        for cpu in arches:
            public, private, computed_repos, generated = self.evidence[target, cpu]
            public_field = field in {'hdrs', 'textual_hdrs'}
            key = target, cpu, public_field
            if key not in self.published_cache:
                self.published_cache[key] = self.graph.visible(target, cpu, public_only=public_field)
            if candidate in self.published_cache[key]:
                continue
            # Unknown directives in that same repository may select headers
            # without spelling them. Retain inputs until their owner supplies
            # its actual textual family via CcInfo.
            if generated or candidate in public or candidate in private or repo in computed_repos:
                return False
        return True

    def text(self):
        result = super().text()
        source = self.source.__class__(result)
        edits = []
        for _, call in source.rules():
            attrs = {kw.arg: kw.value for kw in call.keywords}
            name = attrs.get('name')
            if not isinstance(name, ast.Constant) or not isinstance(name.value, str):
                continue
            target = label(':' + name.value, self.doc.repo)
            for field, wanted in (('deps', self.graph.public), ('implementation_deps', self.graph.private)):
                values = {}
                for cpu in ARCHES:
                    current = self.doc.labels(
                        self.graph.rules[target][2].get(field), cpu
                    ) if target in self.graph.rules else set()
                    values['@platforms//cpu:' + cpu] = sorted(wanted[target, cpu] - current)
                if not any(values.values()):
                    continue
                lists = list(values.values())
                expression = (json.dumps(lists[0], indent=4) if all(v == lists[0] for v in lists)
                              else 'select(' + json.dumps(values, indent=4) + ')')
                if field in attrs:
                    node = attrs[field]
                    edits.append((*source.span(node), '(' + source.text(node) + ') + ' + expression))
                else:
                    at = source.span(call)[1] - 1
                    edits.append((at, at, '\n    ' + field + ' = ' + expression + ',\n'))
        return source.rewrite(edits)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--receipts', type=Path, required=True)
    parser.add_argument('--consumer', action='append', default=[])
    parser.add_argument('--exclude', action='append', default=[])
    parser.add_argument('--revision', help='read BUILD files from this commit; dry run only')
    parser.add_argument('--report', type=Path, help='save candidate dependency evidence')
    parser.add_argument('--compilation-only', action='store_true',
                        help='repair dependencies needed by actual compiled sources only')
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    if args.write and args.revision:
        parser.error('--revision is incompatible with --write')
    sources = Sources(args.receipts, args.root)
    if args.write:
        sources.require_complete()
    paths = [args.root / 'BUILD.bazel',
             *sorted((args.root / 'bazel/dependencies').glob('*.BUILD.bazel'))]
    documents = []
    for path in paths:
        text = None
        if args.revision:
            text = subprocess.check_output(['git', 'show',
                args.revision + ':' + path.relative_to(args.root).as_posix()], cwd=args.root).decode()
        documents.append(Document(path, sources.inventories, text=text))
    graph = DependencyGraph(documents, sources)
    consumers = ((set(args.consumer) or {doc.repo for doc in documents})
                 - set(args.exclude) - SPECIAL_REPOS)
    evidence = graph.prepare(consumers, compilation_only=args.compilation_only)
    changes, owners = Counter(), Counter()
    for doc in documents:
        if doc.repo not in consumers:
            continue
        rewrite = HeaderRewrite(doc, graph, evidence)
        result = rewrite.text()
        if result != doc.source.bytes.decode():
            ast.parse(result, filename=str(doc.path))
            changes.update(rewrite.stats)
            owners.update(rewrite.removed_by_owner)
            changes['files'] += 1
            print(doc.path.relative_to(args.root), dict(rewrite.stats))
            if args.write:
                doc.path.write_text(result)
    print('Dependency evidence:', dict(graph.requests))
    print('Files:', dict(changes), 'written' if args.write else 'no files written')
    print('Removed by owner:', owners.most_common())
    if args.report:
        args.report.write_text(json.dumps({
            'public': [{'target': target, 'cpu': cpu, 'deps': sorted(deps)}
                       for (target, cpu), deps in graph.public.items() if deps],
            'private': [{'target': target, 'cpu': cpu, 'deps': sorted(deps)}
                        for (target, cpu), deps in graph.private.items() if deps],
            'no_provider': [dict(target=target, header=file, cpu=cpu, occurrences=count)
                            for (target, file, cpu), count in graph.unresolved.items()],
        }, indent=2) + '\n')


if __name__ == '__main__':
    main()

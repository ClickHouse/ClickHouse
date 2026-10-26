#!/usr/bin/env python3
"""Reduce external native cc_library deps through already-public dependencies.

A removed label must still be reachable, unchanged, from a remaining direct
dep through cc_library, ch_cc_library, cc_import, or alias public deps, on
both supported CPUs. Public-header witnesses never traverse implementation_deps, source rules, or
unknown macros. External endpoints absent from the overlay graph may be
matched by exact label: the existing native cc_library deps require CcInfo,
but no property of an absent target's own dependencies is assumed.

The default audit writes no BUILD files. --patch writes a proposed diff;
--write replaces files only after the full public graph matches and a second
in-memory pass is unchanged. Non-deps ASTs, root BUILD, and cmake_link inputs
are never edited.
"""

import argparse
import ast
from collections import Counter
import copy
import difflib
from functools import cache
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.compact_build import ast_key
from initial_migration.prune_inherited_headers import (
    ARCHES, CC_PROVIDERS, Document, Graph, label,
)

NATIVE = frozenset({'cc_library', 'ch_cc_library'})
FORWARD = CC_PROVIDERS | {'alias'}
CPU_KEYS = frozenset('@platforms//cpu:' + cpu for cpu in ARCHES)


class PublicDeps:
    def __init__(self, docs):
        self.graph = Graph(docs)
        # Store only public deps (and alias actual), never implementation_deps.
        self.edges = {
            target: {cpu: set(self.graph.dependencies(target, cpu)) for cpu in ARCHES}
            for target, (_, kind, _) in self.graph.rules.items() if kind in FORWARD
        }
        self.order, self.cycles = self.dependencies_first()

    def dependencies_first(self):
        # Tarjan SCCs on the union of both supported configurations. Even a
        # cycle that cannot occur on a single CPU is left alone conservatively.
        index, low, stack, active, order, cycles = {}, {}, [], set(), [], set()

        def visit(target):
            index[target] = low[target] = len(index)
            stack.append(target)
            active.add(target)
            adjacent = set.union(*(self.edges[target][cpu] for cpu in ARCHES))
            for dep in sorted(adjacent & self.edges.keys()):
                if dep not in index:
                    visit(dep)
                    low[target] = min(low[target], low[dep])
                elif dep in active:
                    low[target] = min(low[target], index[dep])
            if index[target] == low[target]:
                component = []
                while True:
                    dep = stack.pop()
                    active.remove(dep)
                    component.append(dep)
                    if dep == target:
                        break
                order.extend(component)
                if len(component) > 1 or target in adjacent:
                    cycles.update(component)

        for target in self.edges:
            if target not in index:
                visit(target)
        return order, cycles

    @cache
    def cc_endpoint(self, target, cpu):
        seen, pending = set(), [target]
        while pending:
            current = pending.pop()
            if current in seen or current in self.cycles:
                return False
            seen.add(current)
            known = self.graph.rules.get(current)
            if known is None:
                continue
            if known[1] not in FORWARD:
                return False
            if known[1] == 'alias':
                actual = self.edges[current][cpu]
                # An alias forwards the provider of actual. If its selection
                # is unknown, or ends at a known non-CcInfo rule, do not use
                # the alias itself to prove public CcInfo coverage.
                if len(actual) != 1:
                    return False
                pending.extend(actual)
        return True

    @cache
    def reachable(self, target, cpu, proof, *, linking=False):
        result, pending = set(), [target]
        while pending:
            current = pending.pop()
            if current in result:
                continue
            if proof and not self.cc_endpoint(current, cpu):
                # Known filegroups, generators, custom rules and cycles never
                # prove that a native CcInfo dependency is redundant.
                continue
            result.add(current)
            if current in self.edges:
                pending.extend(self.edges[current][cpu])
                if linking:
                    doc, _, attrs = self.graph.rules[current]
                    pending.extend(doc.labels(attrs.get("implementation_deps"), cpu))
        return frozenset(result)

    def public(self, target, cpu):
        # Exclude the starting rule unless a real public cycle reaches it.
        return frozenset().union(*(self.reachable(dep, cpu, False) for dep in self.edges[target][cpu]))

    def alternative(self, target, dep, arches):
        if not arches or target in self.cycles or dep in self.cycles:
            return None
        if any(not self.cc_endpoint(dep, cpu) for cpu in arches):
            return None
        witnesses = {}
        for cpu in arches:
            witnesses[cpu] = next((other for other in sorted(self.edges[target][cpu] - {dep})
                                   if dep in self.reachable(other, cpu, True)), None)
        return witnesses if all(witnesses.values()) else None


_LLVM_SOURCE_ONLY_COMPONENTS = {
    '@clickhouse_llvm_project//:' + name for name in (
        'LLVMBitstreamReader', 'LLVMCFGuard', 'LLVMCodeGenTypes', 'LLVMHipStdPar',
        'LLVMIRPrinter', 'LLVMIRReader', 'LLVMSandboxIR',
    )
}


def prune_llvm_link_deps(graph):
    """Leave the LLVM archives on the later dependency that already supplies them."""
    from initial_migration.measured_deps import editable_dependency_lists
    from initial_migration.native_include_deps import explicit_list, rejects_cpu

    source_only, private_headers = set(), set()
    for label, (_, kind, attrs) in graph.rules.items():
        if not label.startswith('@clickhouse_llvm_project//:LLVM') or kind not in NATIVE | {'llvm_cc_library'}:
            continue
        if not {'name', 'srcs'} <= attrs.keys():
            continue
        if attrs.keys() - {'name', 'srcs', 'copts', 'local_defines', 'implementation_deps'}:
            continue
        if label not in _LLVM_SOURCE_ONLY_COMPONENTS:
            paths = [node.value for node in ast.walk(attrs['srcs']) if isinstance(node, ast.Constant)
                     and isinstance(node.value, str) and node.value.startswith('llvm/lib/')]
            if any(p.endswith('.cpp') for p in paths) and any(p.endswith('.h') for p in paths):
                private_headers.add(label)
            continue
        if kind == 'cc_library':
            continue
        # No public headers, defines, includes, or dependencies; no private
        # headers in srcs. Only accept literal .cpp files and .cpp globs.
        srcs = attrs['srcs']
        if isinstance(srcs, ast.Call) and getattr(srcs.func, 'id', '') == 'glob' and len(srcs.args) == 1 and not srcs.keywords:
            srcs = srcs.args[0]
        if isinstance(srcs, ast.List) and srcs.elts and all(
                isinstance(n, ast.Constant) and isinstance(n.value, str) and n.value.endswith('.cpp')
                and not n.value.startswith((':', '/', '@')) for n in srcs.elts):
            source_only.add(label)

    if source_only != _LLVM_SOURCE_ONLY_COMPONENTS:
        source_only.clear()

    def dominated(components, removed, public_only):
        @cache
        def reachable(target, cpu):
            pending, seen = [target], set()
            while pending:
                dep = pending.pop()
                if dep in seen:
                    continue
                seen.add(dep)
                doc, kind, attrs = graph.rules.get(dep, (None, None, {}))
                if kind == 'alias':
                    pending.extend(doc.labels(attrs.get('actual'), cpu))
                elif kind in FORWARD | {'llvm_cc_library'}:
                    pending.extend(doc.labels(attrs.get('deps'), cpu) - removed.get(dep, set()))
                    if not public_only:
                        pending.extend(doc.labels(attrs.get('implementation_deps'), cpu))
            return seen & components

        result = {}
        for target, (doc, kind, attrs) in graph.rules.items():
            deps = attrs.get('deps')
            if doc.repo != '@clickhouse' or kind not in NATIVE | ({'cc_test'} if not public_only else set()):
                continue
            if not explicit_list(deps, doc):
                continue
            candidates = set(components)
            has_group = False
            for cpu in (*ARCHES, 'default'):
                if rejects_cpu(deps, doc, cpu):
                    continue
                direct = [doc.reference(v) for v in doc.values(deps, cpu)]
                if public_only:
                    candidates.intersection_update(direct)
                if public_only or components <= set(direct):
                    has_group = True
                # rules_cc also supplies quote includes for the external repo.
                # Keep the first LLVM library that contributes those include roots.
                earlier_llvm = False
                for dep in direct:
                    if dep in components and not earlier_llvm:
                        candidates.discard(dep)
                    if ((dep not in components or public_only) and dep not in removed.get(target, ())
                            and dep.startswith('@clickhouse_llvm_project//:')
                            and graph.rules.get(dep, (None, None))[1] in NATIVE | {'llvm_cc_library'}):
                        earlier_llvm = True
                # rules_cc visits deps + implementation_deps in reverse for the
                # link. Removing an already-visited archive preserves its position.
                # A header in srcs is still a C++ compilation input of dependents.
                # Require its component through a later *public* dependency as well.
                later = set() if public_only else set().union(*(
                    reachable(v, cpu) for v in doc.labels(attrs.get('implementation_deps'), cpu)))
                for dep in reversed(direct):
                    if dep in components:
                        if dep not in later:
                            candidates.discard(dep)
                    else:
                        later.update(reachable(dep, cpu))
            if has_group and candidates:
                result[target] = candidates
        return result

    # Recheck the witnesses after all proposed direct edges are excluded.
    # Edges skipped by editable_dependency_lists remain available to the linker.
    def removals(removed):
        result = dominated(source_only, removed, False)
        for target, deps in dominated(private_headers, removed, True).items():
            result.setdefault(target, set()).update(deps)
        return result

    proposed = removals({})
    edits = {}
    for target, drop in removals(proposed).items():
        doc, _, attrs = graph.rules[target]
        if changes := editable_dependency_lists(doc, attrs['deps'], drop, doc.reference):
            edits.setdefault(doc, []).extend(changes)
    return edits


def prune_aggregate_deps(root, paths):
    """Preserve header and linker input order when pruning source-free ClickHouse compile groups."""
    from initial_migration.measured_deps import editable_dependency_lists
    from initial_migration.native_include_deps import explicit_list, rejects_cpu

    docs = [Document(p, package=p.parent.relative_to(root).as_posix().removeprefix('.'))
            if p.name == 'BUILD.bazel' else Document(p) for p in sorted(paths)]
    public = PublicDeps(docs)

    def repeated(deps, cpu, linking=False):
        seen, result = set(), set()
        for dep in deps:
            if dep in seen:
                result.add(dep)
            seen.update(public.reachable(dep, cpu, True, linking=linking))
        return result

    edits = {}
    for target, (doc, kind, attrs) in public.graph.rules.items():
        if doc.repo != '@clickhouse' or kind != 'cc_library' or set(attrs) != {'name', 'deps'}:
            continue
        deps = attrs['deps']
        if not explicit_list(deps, doc) or not rejects_cpu(deps, doc, 'default'):
            continue
        redundant = []
        for cpu in ARCHES:
            direct = [doc.reference(d) for d in doc.values(deps, cpu)]
            if not any(d.startswith(target + '__compile_') for d in direct):
                break
            # Headers follow the first public dependency; the linker visits the
            # last dependency first. Require the same provider on both sides.
            redundant.append(repeated(direct, cpu) & repeated(reversed(direct), cpu, linking=True))
        else:
            drop = set.intersection(*redundant)
            if drop and (changes := editable_dependency_lists(doc, deps, drop, doc.reference)):
                edits.setdefault(doc, []).extend(changes)
    for doc, changes in edits.items():
        doc.path.write_text(doc.source.rewrite(changes))

    # Use the remaining aggregate deps as witnesses, never the removed deps.
    if edits:
        docs = [Document(d.path, package=d.package) if d in edits else d for d in docs]
    for doc, changes in prune_llvm_link_deps(Graph(docs)).items():
        doc.path.write_text(doc.source.rewrite(changes))


class Reduction:
    def __init__(self, docs, consumers=()):
        self.docs = docs
        self.public = PublicDeps(docs)
        self.consumers = set(consumers)
        self.edits = {doc.path: [] for doc in docs}
        self.counts = Counter()
        self.skipped = Counter()
        self.details = []

    def expression(self, node, doc, target, arches, constants=()):
        source = doc.source
        original = source.text(node)
        if not arches:
            return original
        if isinstance(node, ast.List):
            if '#' in original:
                self.skipped['commented_lists'] += 1
                return original
            retained = []
            for item in node.elts:
                if not isinstance(item, ast.Constant) or not isinstance(item.value, str):
                    retained.append(source.text(item))
                    continue
                dep = label(item.value, doc.repo)
                witnesses = self.public.alternative(target, dep, arches)
                if witnesses is None:
                    retained.append(source.text(item))
                    continue
                for cpu in arches:
                    self.public.edges[target][cpu].discard(dep)
                self.counts['direct_deps'] += 1
                self.details.append({'target': target, 'dep': dep, 'arches': list(arches),
                                     'via': witnesses, 'line': item.lineno,
                                     'constant': constants[-1] if constants else None})
            if len(retained) < len(node.elts):
                return '[' + ', '.join(retained) + ']'
            return original
        if isinstance(node, ast.Name):
            definition = doc.constants.get(node.id)
            if definition is None or node.id in constants:
                self.skipped['unknown_constants'] += 1
                return original
            value = self.expression(definition, doc, target, arches, (*constants, node.id))
            # Change just this use of a constant. Never edit the constant's
            # other uses in srcs, generator inputs, flags, or unrelated deps.
            if value != source.text(definition):
                return '(' + value + ')'
            return original
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = self.expression(node.left, doc, target, arches, constants)
            right = self.expression(node.right, doc, target, arches, constants)
            if (left, right) == (source.text(node.left), source.text(node.right)):
                return original
            if left in {'[]', '([])'}:
                return right
            if right in {'[]', '([])'}:
                return left
            return '(' + left + ') + (' + right + ')'
        if (isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'select'
                and len(node.args) == 1 and not node.keywords and isinstance(node.args[0], ast.Dict)):
            choices = node.args[0]
            if not all(isinstance(key, ast.Constant) and isinstance(key.value, str)
                       for key in choices.keys):
                self.skipped['unknown_selects'] += 1
                return original
            keys = {key.value for key in choices.keys}
            cpu_only = keys <= CPU_KEYS | {'//conditions:default'}
            edits = []
            for key, value in zip(choices.keys, choices.values):
                selected = arches
                if cpu_only:
                    selected = tuple(cpu for cpu in arches
                                     if key.value == '@platforms//cpu:' + cpu
                                     or key.value == '//conditions:default'
                                     and '@platforms//cpu:' + cpu not in keys)
                updated = self.expression(value, doc, target, selected, constants)
                if updated != source.text(value):
                    edits.append((*source.span(value), updated))
            if not edits:
                return original
            start, end = source.span(node)
            added = sum(len(value.encode()) - (last - first) for first, last, value in edits)
            return source.rewrite(edits).encode()[start:end + added].decode()
        # Glob, formatting, macro calls and every unknown expression remain
        # literal; they cannot be used to assume another dep exists.
        self.skipped['unknown_expressions'] += 1
        return original

    def run(self):
        for target in self.public.order:
            doc, kind, attrs = self.public.graph.rules[target]
            if (doc.repo == '@clickhouse' or kind not in NATIVE or 'deps' not in attrs
                    or self.consumers and doc.repo not in self.consumers):
                continue
            if target in self.public.cycles:
                self.skipped['cyclic_targets'] += 1
                continue
            node = attrs['deps']
            updated = self.expression(node, doc, target, ARCHES)
            if updated != doc.source.text(node):
                self.edits[doc.path].append((*doc.source.span(node), updated))
                self.counts['native_rules'] += 1
                # A later target may already have seen this target in its
                # public dependency closure. Recompute after settling it.
                self.public.reachable.cache_clear()
        result = []
        for doc in self.docs:
            if not self.edits[doc.path]:
                result.append(doc)
                continue
            changed = Document(doc.path, text=doc.source.rewrite(self.edits[doc.path]))
            if non_deps_statements(doc) != non_deps_statements(changed):
                raise ValueError('Changed a non-deps BUILD statement: ' + str(doc.path))
            result.append(changed)
        return result


def non_deps_statements(doc):
    module = copy.deepcopy(doc.source.module)
    for statement in module.body:
        if not isinstance(statement, ast.Expr) or not isinstance(statement.value, ast.Call):
            continue
        call = statement.value
        if getattr(call.func, 'id', '') not in NATIVE:
            continue
        for kw in call.keywords:
            if kw.arg == 'deps':
                kw.value = ast.Constant(value='verified deps')
    return ast_key(module)


def prune(root, *, consumers=(), write=False, patch=None, report=None):
    paths = [root / 'BUILD.bazel', *sorted((root / 'bazel/dependencies').glob('*.BUILD.bazel'))]
    docs = [Document(path) for path in paths]
    reduction = Reduction(docs, consumers)
    after = reduction.run()
    baseline, final = PublicDeps(docs), PublicDeps(after)
    if baseline.graph.rules.keys() != final.graph.rules.keys() or baseline.edges.keys() != final.edges.keys():
        raise ValueError('Rules or CcInfo providers changed')
    verified, digest = Counter(), hashlib.sha256()
    for target in baseline.edges:
        for cpu in ARCHES:
            before, current = baseline.public(target, cpu), final.public(target, cpu)
            if before != current:
                raise ValueError(f'{target} ({cpu}) public dependency closure changed: '
                                 f'lost={sorted(before - current)[:30]}, gained={sorted(current - before)[:30]}')
            digest.update((target + '\0' + cpu + '\0').encode())
            for dep in sorted(before):
                digest.update((dep + '\0').encode())
            verified['rule_cpu_pairs'] += 1
            verified['reachable_target_pairs'] += len(before)
    # A witness selected early in a list may itself become redundant. Record
    # and verify a witness among the final remaining direct deps for every
    # removed literal, including literals in individual select branches.
    for detail in reduction.details:
        target, dep = detail['target'], detail['dep']
        detail['via'] = {
            cpu: next((other for other in sorted(final.edges[target][cpu] - {dep})
                       if dep in final.reachable(other, cpu, True)), None)
            for cpu in detail['arches']
        }
        if not all(detail['via'].values()):
            raise ValueError('Removed dependency has no final public witness: ' + str(detail))
    rerun = Reduction(after, consumers)
    rerun_docs = rerun.run()
    if any(a.source.bytes != b.source.bytes for a, b in zip(after, rerun_docs)):
        raise ValueError('Dependency reduction is not idempotent: ' + str(rerun.counts))
    updates, diff, by_repo = {}, [], Counter()
    for old, new in zip(docs, after):
        if old.source.bytes != new.source.bytes:
            before, result = old.source.bytes.decode(), new.source.bytes.decode()
            updates[old.path] = (before, result)
            rel = str(old.path.relative_to(root))
            diff.extend(difflib.unified_diff(before.splitlines(keepends=True), result.splitlines(keepends=True),
                                             fromfile='a/' + rel, tofile='b/' + rel))
    for detail in reduction.details:
        by_repo[detail['target'].partition('//')[0]] += 1
    data = {'changed_files': len(updates), 'counts': dict(reduction.counts), 'by_repo': dict(by_repo),
            'verified': dict(verified), 'skipped': dict(reduction.skipped),
            'public_closure_sha256': digest.hexdigest(), 'cycles': sorted(baseline.cycles),
            'second_pass_changes': 0, 'details': reduction.details, 'written': write}
    if patch:
        patch.write_text(''.join(diff))
    if report:
        report.write_text(json.dumps(data, sort_keys=True, indent=2) + '\n')
    if write:
        for path, (before, _) in updates.items():
            if path.read_text() != before:
                raise ValueError('BUILD changed during audit: ' + str(path))
        for path, (_, result) in updates.items():
            path.write_text(result)
    return {key: value for key, value in data.items() if key != 'details'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--consumer', action='append', default=[],
                        help='Restrict to this apparent repository, e.g. @clickhouse_thrift')
    parser.add_argument('--patch', type=Path, help='Proposed BUILD-only diff; default writes none')
    parser.add_argument('--report', type=Path, help='Full before/after proof and removed-dep witnesses')
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    print(json.dumps(prune(args.root, consumers=args.consumer, write=args.write,
                           patch=args.patch, report=args.report), indent=2, sort_keys=True))


if __name__ == '__main__':
    main()

"""Remove redundant dependencies from generated ClickHouse and LLVM libraries."""

import ast
from functools import cache

from initial_migration.prune_inherited_headers import ARCHES, CC_PROVIDERS, Document, Graph

NATIVE = frozenset({'cc_library', 'ch_cc_library'})
FORWARD = CC_PROVIDERS | {'alias'}


class PublicDeps:
    def __init__(self, docs):
        self.graph = Graph(docs)
        # Store only public deps (and alias actual), never implementation_deps.
        self.edges = {
            target: {cpu: {self.explicit(dep) for dep in self.graph.dependencies(target, cpu)} for cpu in (*ARCHES, 'default')}
            for target, (_, kind, _) in self.graph.rules.items() if kind in FORWARD
        }
        self.cycles = self.find_cycles()

    def explicit(self, target):
        # Resolve //pkg/sub only when //pkg/sub:sub is a known rule.
        if ':' not in target and (pkg := target.partition('//')[2]):
            full = target + ':' + pkg.rsplit('/', 1)[-1]
            if full in self.graph.rules:
                return full
        return target

    def find_cycles(self):
        # Tarjan SCCs on the union of both supported configurations. Even a
        # cycle that cannot occur on a single CPU is left alone conservatively.
        index, low, stack, active, cycles = {}, {}, [], set(), set()

        def visit(target):
            index[target] = low[target] = len(index)
            stack.append(target)
            active.add(target)
            adjacent = set.union(*self.edges[target].values())
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
                if len(component) > 1 or target in adjacent:
                    cycles.update(component)

        for target in self.edges:
            if target not in index:
                visit(target)
        return cycles

    @cache
    def cc_endpoint(self, target, cpu):
        seen, pending = set(), [target]
        while pending:
            current = self.explicit(pending.pop())
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
    def reachable(self, target, cpu, *, linking=False):
        result, pending = set(), [target]
        while pending:
            current = self.explicit(pending.pop())
            if current in result:
                continue
            if not self.cc_endpoint(current, cpu):
                # Known filegroups, generators, custom rules and cycles never
                # prove that a native CcInfo dependency is redundant.
                continue
            result.add(current)
            if current in self.edges:
                pending.extend(self.edges[current][cpu])
                if linking:
                    doc, kind, attrs = self.graph.rules[current]
                    pending.extend(doc.labels(attrs.get("implementation_deps"), cpu))
                    if (kind == 'ch_cc_library' and getattr(attrs.get('link_runtime'), 'value', None) is True
                            and (cpu == 'default' or 'arches' not in attrs or cpu in doc.values(attrs['arches'], cpu))):
                        pending.append('@clickhouse_llvm_project//:cxx')
        return frozenset(result)


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


def private_compile_groups(doc):
    """Keep trailing compile groups private when they publish no C++ interface."""
    from initial_migration.base_cpp_dependencies import attrs, replace_attrs

    rules = [(call.func.id, attrs(call)) for _, call in doc.source.rules()
             if isinstance(call.func, ast.Name)]
    private = {
        ':' + a['name'].value for kind, a in rules
        if kind in {'cc_library', 'ch_cc_library', 'poco_cc_library'} and 'srcs' in a
        and not {'hdrs', 'textual_hdrs', 'includes', 'defines', 'deps', 'include_prefix',
                 'strip_include_prefix', 'module_interfaces'} & a.keys()
        and isinstance(a['name'], ast.Constant)
    }
    # Moving the 15 glibc C headers changes the inputs of over 5,000 source files per CPU.
    # The assembly group publishes no headers.
    if doc.package == 'base/glibc-compatibility':
        private.discard(':glibc-compatibility__compile_0')
    changes = {}
    for kind, a in rules:
        deps = a.get('deps')
        if (not isinstance(a['name'], ast.Constant) or kind != 'cc_library' or set(a) - {'name', 'deps', 'target_compatible_with'}
                or not isinstance(deps, ast.List) or not all(isinstance(v, ast.Constant) and isinstance(v.value, str) for v in deps.elts)):
            continue
        labels = ast.literal_eval(deps)
        if not any(label.startswith(':' + a['name'].value + '__compile_') for label in labels):
            continue
        split = len(labels)
        while split and labels[split - 1] in private:
            split -= 1
        if split < len(labels):
            changes[a['name'].value] = {
                'deps': repr(labels[:split]) if split else None,
                'implementation_deps': repr(labels[split:]),
            }
    if changes:
        doc.path.write_text(replace_attrs(doc.path.read_text(), changes))


def prune_aggregate_deps(root, paths):
    """Remove repeated compile-group dependencies without changing compiler or linker input order."""
    from initial_migration.measured_deps import editable_dependency_lists
    from initial_migration.native_include_deps import explicit_list, rejects_cpu

    docs = [Document(p, package=p.parent.relative_to(root).as_posix().removeprefix('.'))
            if p.name == 'BUILD.bazel' else Document(p) for p in sorted(paths)]
    public = PublicDeps(docs)

    def repeated(deps, cpu, linking=False, skip=()):
        seen, result = set(), set()
        for dep in deps:
            if dep in seen:
                result.add(dep)
            if dep not in skip:
                seen.update(public.reachable(dep, cpu, linking=linking))
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
    # These cc_libraries own no headers or objects. Their __compile_N targets do.
    fields = ('deps', 'implementation_deps')
    groups = {
        target for target, (doc, kind, attrs) in public.graph.rules.items()
        if kind == 'cc_library' and attrs.keys() <= {'name', *fields}
        and all(any(child.startswith(target + '__compile_')
                    for field in fields for child in doc.labels(attrs.get(field), cpu)) for cpu in ARCHES)
    }
    for _, (doc, kind, attrs) in public.graph.rules.items():
        if doc.repo != '@clickhouse' or kind != 'ch_cc_library' or 'srcs' not in attrs:
            continue
        lists = [attrs[f] for f in fields if f in attrs]
        if not lists or not all(explicit_list(node, doc) for node in lists):
            continue
        cpus = ARCHES if any(rejects_cpu(node, doc, 'default') for node in lists) else (*ARCHES, 'default')
        drop = groups.copy()
        for cpu in cpus:
            direct = [doc.reference(dep) for node in lists for dep in doc.values(node, cpu)]
            if len(set(direct)) != len(direct):
                drop.clear()
                break
            # An earlier public route preserves headers; a later link route preserves objects.
            # Do not use another removable compile-group dependency as the witness.
            drop &= repeated(direct, cpu, skip=groups) & repeated(reversed(direct), cpu, True, groups)
        if drop:
            for node in lists:
                if changes := editable_dependency_lists(doc, node, drop, doc.reference):
                    edits.setdefault(doc, []).extend(changes)
    for doc, changes in edits.items():
        doc.path.write_text(doc.source.rewrite(changes))

    # Use the remaining aggregate deps as witnesses, never the removed deps.
    if edits:
        docs = [Document(d.path, package=d.package) if d in edits else d for d in docs]
    llvm_edits = prune_llvm_link_deps(Graph(docs))
    for doc in docs:
        if doc in llvm_edits:
            doc.path.write_text(doc.source.rewrite(llvm_edits[doc]))
            doc = Document(doc.path, package=doc.package)
        private_compile_groups(doc)

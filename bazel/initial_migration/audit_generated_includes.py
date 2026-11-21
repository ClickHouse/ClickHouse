#!/usr/bin/env python3
"""Audit previously removed generated includes and currently inherited includes.

Source inventories are not inventories of generated files. Index each declared
out/outs, including CODEGEN_RULES, before attempting any include resolution.
An unscanned generator output or a directory with no known generated headers
cannot prove that a generated include was unused.
"""

import argparse
import ast
from collections import Counter, defaultdict, deque
from functools import cache
import io
import json
from pathlib import Path, PurePosixPath
import posixpath
import subprocess
import sys
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.native_cc_inputs import FORCED, Sources, TUS
from initial_migration.compact_build import Source, ast_key
from initial_migration.native_gcp_proto_inputs import (
    codegen_rules_filename, declared_codegen_outputs, dictionary, git_file, string,
)
from initial_migration.native_public_includes import IncludeRewrite, directory, list_elements
from initial_migration.prune_inherited_headers import (
    ARCHES, CC_COMPILERS, CC_PROVIDERS, Document, Graph, label,
)
from initial_migration.repository_inputs import CC_HEADER_SUFFIXES, ROOT

HEADER_SUFFIXES = (*CC_HEADER_SUFFIXES, '.def', '.defs', '.th', '.tpp', '.icc')
# Owners proved by the ea8cda75b audit. Only existing local directories which
# contain these owners' declared generated public headers may be published.
CONFIG_OWNERS = frozenset({
    '@clickhouse_zlib_ng//:_zlib',
    '@clickhouse_bzip2//:_bzip2',
    '@clickhouse_mongo_c_driver//:_libbson',
    '@clickhouse_mongo_c_driver//:_libmongoc',
})


class Package(Document):
    def __init__(self, root, filename, inventories, text):
        super().__init__(root / filename, inventories, text)
        self.package = 'bazel' if filename == 'bazel/BUILD.bazel' else ''

    def labels(self, node, arch):
        result = set()
        for value in self.values(node, arch):
            if self.package and not value.startswith(('@', '//')):
                result.add(self.repo + '//' + self.package + ':' + value.removeprefix(':'))
            else:
                result.add(label(value, self.repo))
        return result

    def target(self, name):
        return self.repo + '//' + self.package + ':' + name


def load_documents(root, revision, inventories):
    codegen = codegen_rules_filename(root, revision)
    raw = subprocess.check_output(
        ['git', 'archive', '--format=tar', revision, 'BUILD.bazel', 'bazel/BUILD.bazel',
         'bazel/dependencies', codegen], cwd=root)
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        files = {member.name: archive.extractfile(member).read().decode()
                 for member in archive.getmembers() if member.isfile() and
                 (member.name in {'BUILD.bazel', 'bazel/BUILD.bazel', codegen}
                  or member.name.endswith('.BUILD.bazel'))}
    names = ['BUILD.bazel', 'bazel/BUILD.bazel'] + sorted(
        name for name in files if name.endswith('.BUILD.bazel'))
    return [Package(root, name, inventories, files[name]) for name in names], files


class GeneratedIncludes(Graph):
    def __init__(self, root, revision, sources, overrides=None):
        self.root, self.revision, self.sources = root, revision, sources
        # Git-pinned main source files, not the selected subset in the include
        # receipt. Model //bazel labels with their real Bazel package.
        self.inventory = set(sources.inventory)
        inventories = dict(sources.inventories)
        root_names = []
        tree = subprocess.check_output(['git', 'ls-tree', '-r', '-z', revision], cwd=root)
        for line in tree.split(b'\0'):
            if not line:
                continue
            meta, name = line.split(b'\t', 1)
            if meta.split()[0] not in {b'100644', b'100755', b'120000'}:
                continue
            name = name.decode()
            if name.startswith('bazel/'):
                self.inventory.add('@clickhouse//bazel:' + name.removeprefix('bazel/'))
            else:
                self.inventory.add('@clickhouse//:' + name)
                root_names.append(name)
        inventories[None] = root_names
        documents, text = load_documents(root, revision, inventories)
        if overrides:
            documents = [Package(root, doc.path.relative_to(root).as_posix(), inventories,
                                 overrides.get(doc.path, doc.source.bytes.decode()))
                         for doc in documents]
        super().__init__(documents)
        # Graph historically assumes one root package per repo. Reconstruct
        # rule labels for //bazel without changing that original class.
        self.rules = {}
        for doc in documents:
            for _, call in doc.source.rules():
                attrs = {kw.arg: kw.value for kw in call.keywords}
                name = attrs['name']
                if isinstance(name, ast.Constant) and isinstance(name.value, str):
                    self.rules[doc.target(name.value)] = doc, getattr(call.func, 'id', ''), attrs
        self.documents = {doc.repo: doc for doc in documents if not doc.package}
        self.all_documents = documents
        self.outputs = defaultdict(set)
        self.rule_outputs = defaultdict(set)
        self.generator_kinds = {}
        for target, (doc, kind, attrs) in self.rules.items():
            if kind not in CC_COMPILERS:
                for cpu in ARCHES:
                    for field in ('out', 'outs'):
                        for out in doc.labels(attrs.get(field), cpu):
                            self.declare(target, kind, out)
        module = ast.parse(text[codegen_rules_filename(root, revision)])
        constants = {statement.targets[0].id: statement.value for statement in module.body
                     if isinstance(statement, ast.Assign) and len(statement.targets) == 1
                     and isinstance(statement.targets[0], ast.Name)}
        shared = dictionary(constants.get('_SHARED_CODEGEN'))
        for name, data in dictionary(constants['CODEGEN_RULES']).items():
            cpu = None
            if (isinstance(data, ast.Call) and getattr(data.func, 'id', '') == 'codegen_for_arch'
                    and len(data.args) == 2 and isinstance(data.args[0], ast.Subscript)):
                cpu = string(data.args[1])
                data = shared[string(data.args[0].slice)]
            # Direct rules already contain the architecture in their output.
            kind, outputs = declared_codegen_outputs(
                name, data, cpu or 'not-an-architecture', lambda p: git_file(root, revision, p).decode())
            if not kind:
                raise ValueError('Unknown codegen ' + name)
            for out in outputs:
                self.declare('@clickhouse//bazel:' + name, kind, '@clickhouse//bazel:' + out)

        self.known_files = frozenset(self.inventory | set(self.outputs))
        self.directives = dict(sources.directives)
        self.register_templates()
        self.by_physical = defaultdict(set)
        for name in self.known_files:
            physical = self.physical(name)
            if physical is not None:
                self.by_physical[physical].add(name)
        self.by_basename = defaultdict(set)
        for filename in self.by_physical:
            self.by_basename[PurePosixPath(filename).name].add(filename)
        self.includes = defaultdict(set)  # Interface used by IncludeRewrite.collect.

    def declare(self, rule, kind, out):
        self.outputs[out].add(rule)
        self.rule_outputs[rule].add(out)
        self.generator_kinds[rule] = kind

    def physical(self, ref):
        repo, sep, rest = ref.partition('//')
        if not sep:
            return None
        package, sep, name = rest.partition(':')
        doc = self.documents.get(repo)
        if not sep or doc is None:
            return None
        return posixpath.normpath(posixpath.join(doc.owner or '', package, name))

    def register_templates(self):
        # Bazel skylib copy_file is a byte copy, including for headers such as
        # gssapi/gssapi.h. Match its includes at the output's physical location,
        # not at the source file location.
        copies = []
        for target, (doc, kind, attrs) in self.rules.items():
            if kind != 'copy_file':
                continue
            inputs = doc.labels(attrs.get('src'), ARCHES[0])
            outputs = self.rule_outputs[target]
            if len(inputs) == 1 and len(outputs) == 1:
                copies.append((next(iter(inputs)), next(iter(outputs))))
        pending = True
        while pending:
            pending = False
            for src, out in copies:
                if out not in self.directives and src in self.directives:
                    self.directives[out] = self.directives[src]
                    pending = True
        for target, (doc, kind, attrs) in self.rules.items():
            if kind != 'expand_template':
                continue
            try:
                values = ast.literal_eval(attrs['substitutions'])
            except (KeyError, ValueError, TypeError):
                continue
            templates = doc.labels(attrs.get('template'), ARCHES[0])
            outputs = self.rule_outputs[target]
            if (len(templates) != 1 or len(outputs) != 1 or not isinstance(values, dict)
                    or any(not isinstance(k, str) or not isinstance(v, str) or
                           any(c in v for c in '\n\r#\\') for k, v in values.items())):
                continue
            template = next(iter(templates))
            if template not in self.directives:
                continue
            rows = []
            for line, directive, syntax, operand in self.directives[template]:
                for key, replacement in values.items():
                    operand = operand.replace(key, replacement)
                rows.append((line, directive, syntax, operand))
            self.directives[next(iter(outputs))] = rows

    def expand(self, refs):
        result = set(refs)
        for ref in refs:
            result.update(self.rule_outputs.get(ref, ()))
        return result

    @cache
    def visible(self, target, cpu, private=True):
        pending = list(self.dependencies(target, cpu))
        if target in self.rules and private:
            doc, kind, attrs = self.rules[target]
            if kind in CC_COMPILERS:
                pending.extend(doc.labels(attrs.get('implementation_deps'), cpu))
        seen = {target}
        while pending:
            ref = pending.pop()
            if ref in seen:
                continue
            seen.add(ref)
            if self.rules.get(ref, (None, None, None))[1] in CC_PROVIDERS | {'alias'}:
                pending.extend(self.dependencies(ref, cpu))
        return frozenset(seen - {target})

    @cache
    def public_files(self, target, cpu):
        rule = self.rules.get(target)
        if not rule or rule[1] not in CC_PROVIDERS:
            return frozenset()
        doc, _, attrs = rule
        return frozenset(self.expand(set().union(*(
            doc.labels(attrs.get(field), cpu) for field in ('hdrs', 'textual_hdrs')
        ))) & self.known_files)

    def element_directory(self, node, doc):
        value = directory(node, doc)
        if value is None:
            return None
        value = value.removeprefix('$(GENDIR)/').removeprefix('$(BINDIR)/')
        foreign = any(isinstance(part, ast.Call) and getattr(part.func, 'id', '') in
                      {'repository_local_include', 'repository_path'} for part in ast.walk(node))
        if not foreign:
            value = posixpath.join(doc.owner or '', doc.package, value)
        return posixpath.normpath(value)

    @cache
    def declared_includes(self, target, cpu):
        if target not in self.rules or self.rules[target][1] not in CC_PROVIDERS:
            return frozenset()
        doc, _, attrs = self.rules[target]
        return frozenset(value for item in list_elements(attrs.get('includes'), doc, cpu)
                         if (value := self.element_directory(item, doc)) is not None)

    @cache
    def compile_roots(self, target, cpu):
        if target not in self.rules:
            return frozenset()
        doc, _, attrs = self.rules[target]
        roots = set()
        for field in ('local_includes', 'includes', 'copts'):
            opts = list_elements(attrs.get(field), doc, cpu)
            index = 0
            while index < len(opts):
                item = opts[index]
                index += 1
                if field == 'copts':
                    value = directory(item, doc)
                    if value in {'-I', '-isystem', '-iquote'} and index < len(opts):
                        item = opts[index]
                        index += 1
                    else:
                        continue
                value = self.element_directory(item, doc)
                if value is not None:
                    roots.add(value)
        for ref in self.visible(target, cpu):
            roots.update(self.declared_includes(ref, cpu))
        return frozenset(roots)

    @cache
    def compile_files(self, target, cpu):
        if target not in self.rules:
            return frozenset()
        doc, _, attrs = self.rules[target]
        result = self.expand(set().union(*(doc.labels(attrs.get(field), cpu) for field in
                                         ('srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs'))))
        for ref in self.visible(target, cpu):
            result.update(self.public_files(ref, cpu))
        return frozenset(result)

    @cache
    def directory_files(self, absolute):
        prefix = absolute.rstrip('/') + '/'
        return frozenset(ref for filename, refs in self.by_physical.items()
                         if filename.startswith(prefix) for ref in refs
                         if ref.endswith(HEADER_SUFFIXES))

    def match(self, source, operand, syntax, roots, available):
        # A known quoted sibling resolves before include paths. Generator
        # outputs participate even though they are absent from source receipts.
        physical = self.physical(self.sources.source_copies.get(source, source))
        if syntax in {'quote', 'apostrophe'} and physical:
            sibling = posixpath.normpath(posixpath.join(posixpath.dirname(physical), operand))
            if sibling in self.by_physical:
                return self.by_physical[sibling]
        result = set()
        for root in roots:
            name = posixpath.normpath(posixpath.join(root, operand))
            result.update(self.by_physical.get(name, ()))
        # Keep matching files explicitly declared to this compilation, even
        # if the migration previously dropped their include directory.
        basename = PurePosixPath(operand).name
        for filename in self.by_basename.get(basename, ()):
            if filename == operand or filename.endswith('/' + operand):
                result.update(self.by_physical[filename] & available)
        return result

    @cache
    def trace(self, target, cpu):
        doc, _, attrs = self.rules[target]
        available = self.compile_files(target, cpu)
        include_roots = self.compile_roots(target, cpu)
        inputs = self.expand(doc.labels(attrs.get('srcs'), cpu))
        prefix = (doc.owner or '') + '/'
        # Own physical public headers and public generator outputs, not raw
        # foreign source lists captured from some other compilation.
        root_config = 'contrib/' + doc.owner.removeprefix('contrib/') + '-cmake/' if doc.owner else None
        headers = set()
        for ref in self.public_files(target, cpu):
            if not ref.endswith(HEADER_SUFFIXES):
                continue
            physical = self.physical(ref) or ''
            own_source_or_config = physical.startswith(prefix)
            own_root_generated = (ref in self.outputs and root_config is not None and
                                  any(physical.startswith('bazel/gen/' + arch + '/' + root_config)
                                      for arch in ARCHES))
            if own_source_or_config or own_root_generated:
                headers.add(ref)
        pending = deque((ref, 'public' if ref in headers else 'private')
                        for ref in headers | {ref for ref in inputs if ref.endswith(TUS)})
        opts = doc.values(attrs.get('copts'), cpu)
        for index, opt in enumerate(opts[:-1]):
            if opt in FORCED:
                for ref in self.match(target, opts[index + 1], 'quote', include_roots, available):
                    pending.append((ref, 'private'))
        reached, computed, unknown, includes = set(), [], [], []
        while pending:
            ref, access = pending.popleft()
            if (ref, access) in reached:
                continue
            reached.add((ref, access))
            if ref not in self.directives:
                if ref in self.outputs and ref.endswith((*HEADER_SUFFIXES, *TUS)):
                    unknown.append(dict(file=ref, access=access, producers=sorted(self.outputs[ref])))
                continue
            for line, directive, syntax, operand in self.directives[ref]:
                if syntax == 'computed':
                    computed.append(dict(file=ref, line=line, operand=operand))
                    continue
                for selected in self.match(ref, operand, syntax, include_roots, available):
                    includes.append((ref, line, operand, selected, access))
                    pending.append((selected, access))
        return dict(reached=reached, computed=computed, unknown=unknown, includes=includes)

    def provider(self, target, value, cpu):
        """Return a provider only for an existing exact public include and files."""
        if target not in self.rules:
            return None
        doc, _, _ = self.rules[target]
        wanted = self.element_directory(value, doc)
        if wanted is None:
            return None
        for provider in sorted(self.visible(target, cpu)):
            if wanted in self.declared_includes(provider, cpu) and (
                    self.directory_files(wanted) & self.public_files(provider, cpu)):
                return provider, None
        return None

    @cache
    def native_config_owners(self, absolute, cpu):
        """A configuration header can be owned only in its declaring package.

        Do not nominate a target which happens to capture another repository's
        generated files. Do not invent one owner for bazel/gen/<arch>.
        """
        files = self.directory_files(absolute) & set(self.outputs)
        if absolute in {'bazel/gen/' + arch for arch in ARCHES}:
            return (), 'requires_specific_generator_or_schema_trace'
        if not files:
            return (), 'no_declared_outputs_in_removed_directory'
        packages = {file.partition('//')[0] + '//' + file.partition('//')[2].partition(':')[0]
                    for file in files}
        candidates = []
        for owner, (doc, kind, attrs) in self.rules.items():
            if kind not in CC_PROVIDERS or owner.partition(':')[0] not in packages:
                continue
            published = self.public_files(owner, cpu) & files
            if not published:
                continue
            # Prefer the exact local include already used by the owner.
            # A public directory must resolve within the owner's real package.
            relative = posixpath.relpath(absolute, posixpath.join(doc.owner or '', doc.package) or '.')
            if relative == '..' or relative.startswith('../'):
                continue
            local = {self.element_directory(node, doc)
                     for node in list_elements(attrs.get('local_includes'), doc, cpu)}
            candidates.append(dict(target=owner, relative_include=relative,
                already_public=absolute in self.declared_includes(owner, cpu),
                already_local=absolute in local, public_generated_headers=sorted(published)))
        return tuple(candidates), 'existing_native_owner' if candidates else 'no_native_declaring_owner'


def without_owner_local_includes(doc, graph):
    """Remove published literals locally, including from shared constants.

    A shared constant is expanded only in this owner's local_includes when its
    contents change. Other rules and other attributes still use the constant.
    """
    source, edits = doc.source, []
    for _, call in source.rules():
        attrs = {kw.arg: kw.value for kw in call.keywords}
        name, value = attrs.get('name'), attrs.get('local_includes')
        if not isinstance(name, ast.Constant) or value is None:
            continue
        target = doc.target(name.value)
        if target not in CONFIG_OWNERS:
            continue
        permitted, seen = defaultdict(set), defaultdict(set)
        for cpu in ARCHES:
            for element in list_elements(value, doc, cpu):
                seen[id(element)].add(cpu)
                if (isinstance(element, ast.Constant) and isinstance(element.value, str)
                        and element.value in graph.includes[target, cpu]):
                    permitted[id(element)].add(cpu)
        removed = {key for key in permitted if permitted[key] == seen[key]}

        def part(node, stack=()):
            original = source.text(node)
            if isinstance(node, ast.Name) and node.id in doc.constants and node.id not in stack:
                replacement = part(doc.constants[node.id], (*stack, node.id))
                if replacement != source.text(doc.constants[node.id]):
                    return '(' + replacement + ')'
                return original
            if isinstance(node, (ast.List, ast.Tuple)):
                keep = [item for item in node.elts if id(item) not in removed]
                if len(keep) != len(node.elts):
                    return '[' + ', '.join(source.text(item) for item in keep) + ']'
                return original
            children = []
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
                children = [node.left, node.right]
            elif (isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'select'
                  and len(node.args) == 1 and isinstance(node.args[0], ast.Dict)):
                children = node.args[0].values
            if not children:
                return original
            first, _ = source.span(node)
            result = original.encode()
            for child in sorted(children, key=lambda item: source.span(item)[0], reverse=True):
                lo, hi = source.span(child)
                result = result[:lo - first] + part(child, stack).encode() + result[hi - first:]
            return result.decode()

        replacement = part(value)
        if replacement != source.text(value):
            edits.append((*source.span(value), replacement))
    return source.rewrite(edits)


def include_cleanup(root, graph, sources, groups, rewrites, write):
    """Plan only proved native configuration publications and inherited RLIs."""
    published = defaultdict(set)
    for group in groups:
        for use in group['uses']:
            for owner in use['native_config_owners']:
                if (owner['target'] in CONFIG_OWNERS and owner['visible']
                        and owner['already_local'] and not owner['already_public']
                        and owner['public_generated_headers']):
                    published[owner['target'], use['cpu']].add(owner['relative_include'])
    graph.includes.update(published)
    pending, proofs = {}, []
    original_docs = {doc.repo: doc for doc in graph.all_documents if not doc.package}
    for rewrite in rewrites:
        doc = rewrite.doc
        rendered = rewrite.text()
        owned_doc = Package(root, doc.path.relative_to(root).as_posix(), sources.inventories, rendered)
        rendered = without_owner_local_includes(owned_doc, graph)
        if rendered == doc.source.bytes.decode():
            continue
        formatted = subprocess.run(
            ['buildifier', '-config=' + str(root / '.buildifier.json'), '-type=build'],
            input=rendered, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True,
        ).stdout
        expected_source, actual_source = Source(rendered), Source(formatted)
        # Formatting can reorder named arguments, which has no Bazel meaning;
        # it must not change list order or any expression.
        for source in (expected_source, actual_source):
            for node in ast.walk(source.module):
                if isinstance(node, ast.Call):
                    node.keywords.sort(key=lambda attr: attr.arg or '')
        if ast_key(expected_source.module) != ast_key(actual_source.module):
            raise ValueError('Formatting changes expressions or list order: ' + str(doc.path))
        pending[doc.path] = formatted
    actual = GeneratedIncludes(root, graph.revision, sources, pending)
    checked_targets = checked_arches = 0
    for target, (doc, kind, attrs) in graph.rules.items():
        after_doc, after_kind, after_attrs = actual.rules[target]
        if kind != after_kind or set(attrs) - {'includes', 'local_includes'} != set(after_attrs) - {
                'includes', 'local_includes'}:
            raise ValueError('Changed rule or attribute set: ' + target)
        for field, node in attrs.items():
            if field not in {'includes', 'local_includes'} and ast_key(node) != ast_key(after_attrs[field]):
                raise ValueError('Changed ' + field + ' in ' + target)
        if doc.path not in pending or kind not in CC_PROVIDERS | CC_COMPILERS:
            continue
        checked_targets += 1
        for cpu in ARCHES:
            checked_arches += 1
            old_local = list_elements(attrs.get('local_includes'), doc, cpu)
            eligible = {id(site['item']) for rewrite in rewrites if rewrite.doc is doc
                        for site in rewrite.sites.values() if site['eligible']}
            expected_local = [
                ast_key(item) for item in old_local
                if id(item) not in eligible and not (
                    target in CONFIG_OWNERS and isinstance(item, ast.Constant)
                    and item.value in published[target, cpu])
            ]
            current_local = [ast_key(item) for item in
                             list_elements(after_attrs.get('local_includes'), after_doc, cpu)]
            if expected_local != current_local:
                raise ValueError('Unexpected local_includes/order: ' + target + ' ' + cpu)
            old_public = [ast_key(item) for item in list_elements(attrs.get('includes'), doc, cpu)]
            expected_public = old_public + [ast_key(ast.Constant(value)) for value
                                            in sorted(published[target, cpu])]
            current_public = [ast_key(item) for item in
                              list_elements(after_attrs.get('includes'), after_doc, cpu)]
            if expected_public != current_public:
                raise ValueError('Unexpected includes/order: ' + target + ' ' + cpu)
            if graph.public_files(target, cpu) != actual.public_files(target, cpu):
                raise ValueError('Changed public source or generated headers: ' + target + ' ' + cpu)
            lost = graph.compile_roots(target, cpu) - actual.compile_roots(target, cpu)
            if lost:
                raise ValueError('Compilation no longer has include roots: ' + target + ' ' + cpu + str(lost))
    for rewrite in rewrites:
        for site in rewrite.sites.values():
            if not site['eligible']:
                continue
            for target, cpu, _ in site['uses']:
                supplied = actual.provider(target, site['item'], cpu)
                if not supplied:
                    raise ValueError('Removed RLI without visible exact public include: ' + target + ' ' + cpu)
    for (target, cpu), directories in sorted(published.items()):
        doc = original_docs[target.partition('//')[0]]
        for relative in sorted(directories):
            absolute = posixpath.normpath(posixpath.join(doc.owner, relative))
            declared = (actual.public_files(target, cpu) & actual.directory_files(absolute)
                        & set(actual.outputs))
            if not declared or absolute not in actual.declared_includes(target, cpu):
                raise ValueError('Published no declared header output: ' + target + ' ' + absolute)
            proofs.append(dict(target=target, cpu=cpu, relative=relative,
                               declared_generated_headers=sorted(declared)))
    if set(graph.rules) != set(actual.rules):
        raise ValueError('Named targets changed')
    if write:
        # Proofs depend on every document in the graph, not only edited files.
        # Refuse to use a stale revision if any BUILD has been edited since.
        for doc in graph.all_documents:
            if doc.path.read_bytes() != doc.source.bytes:
                raise ValueError('BUILD changed since audit snapshot: ' + str(doc.path))
        for path, formatted in pending.items():
            path.write_text(formatted)
    return dict(written=write, files=[str(path.relative_to(root)) for path in pending],
                checked_named_targets=len(graph.rules), checked_compile_targets=checked_targets,
                checked_target_arches=checked_arches, publications=proofs,
                removed_rli_sites=sum(site['eligible'] for rewrite in rewrites for site in rewrite.sites.values()))


def audit(root, revision, receipts, prior_receipt, output, cleanup=False, write=False, excluded=()):
    sources = Sources(receipts, root)
    sources.require_complete()
    receipt = json.loads(prior_receipt.read_text())
    before = GeneratedIncludes(root, receipt['revision'], sources)
    after = GeneratedIncludes(root, revision, sources)
    groups = []
    counts = Counter()
    for removed in receipt['removed']:
        expression = removed['expression']
        if not any(text in expression for text in ('$(GENDIR)', '$(BINDIR)', '/config/')):
            continue
        record = dict(file=removed['file'], old_line=removed['line'], expression=expression, uses=[])
        for use in removed['consumers']:
            target, cpu = use['target'], use['cpu']
            absolute = use['directory'].removeprefix('$(GENDIR)/').removeprefix('$(BINDIR)/')
            absolute = posixpath.normpath(absolute)
            prior_files = before.directory_files(absolute)
            outputs = prior_files & set(before.outputs)
            current_files = after.directory_files(absolute)
            visible = after.visible(target, cpu)
            providers = {ref: after.public_files(ref, cpu) & current_files for ref in visible
                         if absolute in after.declared_includes(ref, cpu)}
            providers = {ref: files for ref, files in providers.items() if files}
            config_owners, owner_status = after.native_config_owners(absolute, cpu)
            trace = before.trace(target, cpu)
            # Belonging anywhere under the old directory is insufficient:
            # "mongoc/mongoc-config.h" is found through the parent src dir,
            # not through src/mongoc. Require the exact include root+operand.
            demands = [(source, line, operand, ref, access)
                       for source, line, operand, ref, access in trace['includes']
                       if ref in prior_files and before.physical(ref) ==
                       posixpath.normpath(posixpath.join(absolute, operand))]
            demand_refs = {row[3] for row in demands}
            published = set().union(*providers.values())
            missing = demand_refs - published
            # The schema or generator output can require headers which cannot
            # be learned from source receipts. Never report that as unused.
            if providers and not missing:
                status = 'existing_owner_supplies_include'
            elif demands:
                status = 'missing_generated_include_or_public_header'
            elif not outputs:
                status = 'no_declared_outputs_cannot_prove_unused'
            elif trace['unknown'] or trace['computed']:
                status = 'uninspected_output_or_computed_include_cannot_prove_unused'
            else:
                status = 'no_literal_demand_in_complete_trace'
            counts[status] += 1
            record['uses'].append(dict(target=target, cpu=cpu, absolute_directory=absolute, status=status,
                source_headers=len(prior_files - outputs), declared_headers=len(outputs),
                inventory_class=('generated_and_source' if outputs and prior_files - outputs else
                                 'generated' if outputs else
                                 'source_only' if prior_files else 'empty'),
                declared_output_examples=[dict(file=f, producers=sorted(before.outputs[f]))
                                          for f in sorted(outputs)[:8]],
                demand_count=len(demands), missing_headers=sorted(missing),
                demand_examples=[dict(source=s, line=l, operand=o, output=r, access=a)
                                 for s, l, o, r, a in demands[:8]],
                providers=[dict(target=p, public_files=sorted(files)) for p, files in providers.items()],
                owner_status=owner_status,
                native_config_owners=[dict(candidate, visible=candidate['target'] in visible,
                    recommendation=('already_supplies_directory' if candidate['already_public'] else
                                    'can_publish_existing_local_include' if candidate['already_local']
                                    and candidate['target'] in visible else
                                    'need_demonstrated_header_use_before_adding_dependency'))
                    for candidate in config_owners],
                unknown_outputs=len(trace['unknown']), unknown_output_examples=trace['unknown'][:8],
                computed_operands=len(trace['computed']), computed_examples=trace['computed'][:5]))
        groups.append(record)
    covered, blocked, rewrites = [], Counter(), []
    for doc in after.all_documents:
        if doc.repo == '@clickhouse':
            continue
        rewrite = IncludeRewrite(doc, after)
        rewrite.collect()
        rewrites.append(rewrite)
        for site in rewrite.sites.values():
            item = site['item']
            if site['eligible']:
                covered.append(dict(file=str(doc.path.relative_to(root)), line=item.lineno,
                    expression=doc.source.text(item),
                    uses=[dict(target=t, cpu=c, directory=d, provider=after.provider(t, item, c)[0])
                          for t, c, d in sorted(site['uses'])]))
            else:
                blocked[doc.repo] += 1
    result = dict(revision=revision, previous_revision=receipt['revision'],
                  removed_generated_sites=len(groups), historical_status=dict(counts),
                  current_covered_sites=len(covered), current_blocked_sites=dict(blocked),
                  removed_generated=groups, current_covered=covered)
    if cleanup:
        selected = [rewrite for rewrite in rewrites
                    if rewrite.doc.path.relative_to(root).as_posix() not in excluded]
        result['cleanup'] = include_cleanup(root, after, sources, groups, selected, write)
    output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({key: value for key, value in result.items()
                      if key not in {'removed_generated', 'current_covered', 'current_blocked_sites'}}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--receipts', type=Path, required=True)
    parser.add_argument('--prior-receipt', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--cleanup', action='store_true',
                        help='Plan native config publications and exact-covered RLI removals')
    parser.add_argument('--write', action='store_true',
                        help='Apply --cleanup after checking all BUILD files still match --revision')
    parser.add_argument('--exclude-build', action='append', default=[],
                        help='Repository-relative BUILD overlay to leave for another migration')
    args = parser.parse_args()
    audit(args.root, args.revision, args.receipts, args.prior_receipt, args.output,
          cleanup=args.cleanup or args.write, write=args.write, excluded=tuple(args.exclude_build))


if __name__ == '__main__':
    main()

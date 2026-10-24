#!/usr/bin/env python3
"""Remove source files copied to unrelated native compiler actions.

Require SHA-verified source and include receipts. A source compiled in another
target does not make it an include in this target. Preserve unknown generated
translation units, computed include names, forced includes and real textual
inclusions. Genrules and runtime-specific rules retain their declared inputs.
"""
import argparse
import ast
import base64
from collections import Counter, defaultdict, deque
from functools import cache
import hashlib
import json
from pathlib import Path, PurePosixPath
import posixpath
import re
import shlex
import subprocess
import sys
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.native_cc_dependencies import DependencyGraph
from initial_migration.native_cc_inputs import FORCED, Sources, SPECIAL_REPOS, TUS
from initial_migration.native_gcp_proto_inputs import (
    GCP, GRPC, GRPC_PLUGIN, PROTOBUF, PROTOC, PROTO_IMPORT,
    GoogleProtos, codegen_rules_filename, dictionary, git_file, string, strings,
)
from initial_migration.prune_inherited_headers import (
    ARCHES, CC_COMPILERS, CC_PROVIDERS, Document, Rewrite, label,
)
from initial_migration.repository_inputs import CC_HEADER_SUFFIXES, REPOSITORIES, ROOT


def libcxx_headers(root, archive, registry, inventory):
    """Headers owned by the exact LLVM module selected by MODULE.bazel.lock."""
    if not archive:
        return frozenset()
    module = ast.parse((root / 'MODULE.bazel').read_text())
    version = next(ast.literal_eval(kw.value)
                   for node in ast.walk(module)
                   if isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'bazel_dep'
                   and any(kw.arg == 'name' and ast.literal_eval(kw.value) == 'llvm' for kw in node.keywords)
                   for kw in node.keywords if kw.arg == 'version')
    lock = json.loads((root / 'MODULE.bazel.lock').read_text())
    source_key = 'https://bcr.bazel.build/modules/llvm/' + version + '/source.json'
    if hashlib.sha256(registry.read_bytes()).hexdigest() != lock['registryFileHashes'][source_key]:
        raise ValueError('LLVM source.json is not the one in MODULE.bazel.lock')
    spec = json.loads(registry.read_text())
    digest = base64.b64encode(hashlib.sha256(archive.read_bytes()).digest()).decode()
    if 'sha256-' + digest != spec['integrity']:
        raise ValueError('LLVM module archive does not match source.json')
    llvm_module = ast.parse((root / 'bazel/llvm.MODULE.bazel').read_text())
    llvm_version = next(ast.literal_eval(kw.value)
                        for node in ast.walk(llvm_module) if isinstance(node, ast.Call)
                        and getattr(node.func, 'attr', '') == 'version'
                        for kw in node.keywords if kw.arg == 'llvm_version')
    llvm_sha = next(ast.literal_eval(kw.value)
                   for node in ast.walk(llvm_module) if isinstance(node, ast.Call)
                   and getattr(node.func, 'attr', '') == 'from_archive'
                   for kw in node.keywords if kw.arg == 'sha256')
    import gzip
    with gzip.open(inventory, 'rt') as file:
        pinned = json.load(file)['contrib/llvm-project']
    if pinned['sha256'] != llvm_sha or llvm_sha != REPOSITORIES['contrib/llvm-project']['sha256']:
        raise ValueError('LLVM source inventory does not match llvm.MODULE.bazel')
    major = llvm_version.partition('.')[0]
    with tarfile.open(archive) as tar:
        filename = spec['strip_prefix'] + '/3rd_party/llvm-project/' + major + '.x/libcxx/libcxx_headers.bzl'
        module = ast.parse(tar.extractfile(filename).read().decode())
        files = next(ast.literal_eval(node.value)
                     for node in module.body if isinstance(node, ast.Assign)
                     and any(getattr(t, 'id', '') == 'LIBCXX_HEADERS_FILES_' + major for t in node.targets))
    owned = {'libcxx/' + path for path in files} & set(pinned['files'])
    return frozenset(path.removeprefix('libcxx/include/') for path in owned
                     if path.startswith('libcxx/include/'))


def string_values(doc, node, cpu):
    """Resolve include roots without interpreting unrelated macros as flags."""
    if node is None:
        return []
    if isinstance(node, ast.Constant):
        return node.value
    if (doc.owner and isinstance(node, ast.Attribute) and node.attr == 'workspace_root'
            and isinstance(node.value, ast.Call) and getattr(node.value.func, 'id', '') == 'package_relative_label'
            and len(node.value.args) == 1 and not node.value.keywords
            and (string(node.value.args[0]) or '').startswith(':')):
        return '@SOURCE@/' + doc.owner
    if isinstance(node, ast.Name) and node.id in doc.constants:
        return string_values(doc, doc.constants[node.id], cpu)
    if isinstance(node, (ast.List, ast.Tuple)):
        return [string_values(doc, value, cpu) for value in node.elts]
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        lhs, rhs = string_values(doc, node.left, cpu), string_values(doc, node.right, cpu)
        if isinstance(lhs, str) and isinstance(rhs, str) or isinstance(lhs, list) and isinstance(rhs, list):
            return lhs + rhs
        return None
    if isinstance(node, ast.Call):
        func = getattr(node.func, 'id', '')
        if func == 'repository_path' and len(node.args) == 1:
            owner = string_values(doc, node.args[0], cpu)
            return '@SOURCE@/' + owner if isinstance(owner, str) else None
        if func == 'repository_local_include' and len(node.args) == 2:
            path = string_values(doc, node.args[0], cpu)
            if isinstance(path, str):
                return path if '@SOURCE@/' in path or '$(GENDIR)' in path else '@SOURCE@/' + path
        if func == 'cmake_prefix_maps':
            # cmake_prefix_maps returns only -ffile-prefix-map options.
            return []
        if func == 'select' and len(node.args) == 1 and isinstance(node.args[0], ast.Dict):
            choice = node.args[0]
            table = {key.value: val for key, val in zip(choice.keys, choice.values)
                     if isinstance(key, ast.Constant)}
            keys = {'@platforms//cpu:' + arch for arch in ARCHES} | {'//conditions:default'}
            if len(table) == len(choice.keys) and set(table) <= keys:
                return string_values(doc, table.get('@platforms//cpu:' + cpu,
                                                     table.get('//conditions:default')), cpu)
    return None


class TextualGraph(DependencyGraph):
    def __init__(self, documents, sources, cpp_headers):
        super().__init__(documents, sources)
        self.cpp_headers = cpp_headers

    @cache
    def include_directories(self, target, cpu):
        """Actual source include directories in this target and public deps."""
        doc, _, attrs = self.rules[target]
        pending = [(target, True)]
        pending += [(dep, False) for dep in self.dependencies(target, cpu)]
        pending += [(dep, False) for dep in self.private_dependencies(target, cpu)]
        dirs, seen = set(), set()
        while pending:
            rule, own = pending.pop()
            if rule in seen or rule not in self.rules:
                continue
            seen.add(rule)
            doc, kind, attrs = self.rules[rule]
            if kind not in CC_COMPILERS | {'alias'}:
                continue
            fields = ('includes', 'local_includes', 'copts') if own else ('includes',)
            for field in fields:
                vals = string_values(doc, attrs.get(field), cpu)
                if not isinstance(vals, list) or any(not isinstance(x, str) for x in vals):
                    return None
                if field == 'copts':
                    # Keep unknown option spellings such as -isystem=...;
                    # they cannot prove a filename cannot be searched.
                    include_opts = ('-I', '-iquote', '-isystem', '-idirafter')
                    paths = []
                    for i, val in enumerate(vals):
                        if val in include_opts:
                            if i + 1 >= len(vals):
                                return None
                            paths.append(vals[i + 1])
                        elif val.startswith(include_opts) and not val.startswith('-I-'):
                            prefix = next(opt for opt in include_opts if val.startswith(opt))
                            paths.append(val.removeprefix(prefix).lstrip('='))
                else:
                    paths = vals
                for path in paths:
                    if '$(GENDIR)' in path or path.startswith('bazel-out/'):
                        continue
                    if path.startswith('@SOURCE@/'):
                        dirs.add(posixpath.normpath(path.removeprefix('@SOURCE@/')))
                    elif path.startswith(('/', '$(')):
                        return None
                    else:
                        dirs.add(posixpath.normpath(posixpath.join(doc.owner or '', path)))
            pending.extend((dep, False) for dep in self.dependencies(rule, cpu))
        return frozenset(dirs)

    @cache
    def used(self, target, cpu, public_only):
        if not self.cpp_headers:
            return super().used(target, cpu, public_only)
        doc, kind, attrs = self.rules[target]
        language = string_values(doc, attrs.get('language'), cpu) if 'language' in attrs else 'CXX'
        if kind != 'ch_cc_library' or language != 'CXX':
            return super().used(target, cpu, public_only)
        extras = {
            ref for ref in doc.labels(attrs.get('additional_compiler_inputs'), cpu)
            if ref in self.sources.inventory
            and ref.partition('//:')[0] != doc.repo
            and PurePosixPath(ref.partition('//:')[2]).name in self.cpp_headers
        }
        if not extras:
            return super().used(target, cpu, public_only)
        roots = set(self.own_headers(target, cpu)) if public_only else set(self.archive_sources(target, cpu))
        if not public_only:
            opts = doc.values(attrs.get('copts'), cpu)
            for i, value in enumerate(opts[:-1]):
                if value in FORCED:
                    roots.update(self.sources.include_candidates(target, opts[i + 1], 'quote'))
        available = self.compile_files(target, cpu)
        if not self.archive_sources(target, cpu):
            available |= self.repository_compile_files(doc.repo, cpu)
        pending, live, computed = deque(roots), set(), set()
        while pending:
            path = pending.popleft()
            if path in live:
                continue
            live.add(path)
            for _, _, syntax, operand in self.sources.directives.get(path, ()):
                if syntax == 'computed':
                    computed.add(path.partition('//:')[0])
                    continue
                matches = self.sources.include_candidates(path, operand, syntax)
                candidates = set(matches) & available
                candidates.update(ref for ref in matches
                                  if self.under_original_include(target, cpu, ref, operand))
                if syntax == 'angle' and operand in self.cpp_headers and candidates & extras:
                    dirs = self.include_directories(target, cpu)
                    if dirs is not None:
                        for candidate in candidates & extras:
                            repo, _, filename = candidate.partition('//:')
                            owner = self.documents.get(repo)
                            if owner is None or owner.owner is None:
                                continue
                            actual = posixpath.join(owner.owner, filename)
                            if not any(posixpath.normpath(posixpath.join(directory, operand)) == actual
                                       for directory in dirs):
                                candidates.remove(candidate)
                pending.extend(ref for ref in candidates if ref not in live)
        return frozenset(live), frozenset(computed)


class NativeProtos(GoogleProtos):
    """Verify single-schema protoc commands against pinned source bodies.

    The protoc arguments determine the schema and its import directories.
    Extra genrule inputs are not implicitly schema imports.
    """

    def __init__(self, root, revision, graph, sources, archives, working_tree=False):
        super().__init__(root, revision, graph, sources)
        self.root, self.revision, self.working_tree = root, revision, working_tree
        self.regular_outputs = {}
        self.regular_schemas = {}
        self.physical_schemas = defaultdict(set)
        for ref in sources.inventory:
            repo, sep, filename = ref.partition('//:')
            if sep and filename.endswith('.proto') and repo in graph.documents:
                owner = graph.documents[repo].owner or ''
                self.physical_schemas[posixpath.join(owner, filename)].add(ref)
        for item in archives:
            owner, sep, filename = item.partition('=')
            if not sep or owner not in REPOSITORIES:
                raise ValueError('--proto-archive must be a configured contrib/owner=archive')
            spec, path = REPOSITORIES[owner], Path(filename)
            if hashlib.sha256(path.read_bytes()).hexdigest() != spec['sha256']:
                raise ValueError('Protobuf schema archive does not match pinned SHA256: ' + owner)
            with tarfile.open(path) as archive:
                for member in archive.getmembers():
                    prefix = spec['strip_prefix'] + '/'
                    if not member.isfile() or not member.name.startswith(prefix) or not member.name.endswith('.proto'):
                        continue
                    ref = '@' + spec['name'] + '//:' + member.name.removeprefix(prefix)
                    if ref in sources.inventory:
                        self.regular_schemas[ref] = archive.extractfile(member).read().decode()

        tree = ast.parse(git_file(root, revision, codegen_rules_filename(root, revision)))
        constants = {
            stmt.targets[0].id: stmt.value for stmt in tree.body
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1
            and isinstance(stmt.targets[0], ast.Name)
        }
        shared = dictionary(constants.get('_SHARED_CODEGEN'))
        for name, rule in dictionary(constants.get('CODEGEN_RULES')).items():
            if not (isinstance(rule, ast.Call) and getattr(rule.func, 'id', '') == 'codegen_for_arch'
                    and len(rule.args) == 2 and isinstance(rule.args[0], ast.Subscript)):
                continue
            template, cpu = string(rule.args[0].slice), string(rule.args[1])
            if template not in shared or cpu not in ARCHES:
                continue
            attrs = dictionary(shared[template])
            outs = strings(attrs.get('outs'), cpu)
            record = self.regular_generator(name, cpu, attrs, outs)
            if record:
                for output in record['outputs']:
                    self.regular_outputs[output] = record

        for target, (doc, kind, attrs) in graph.rules.items():
            if kind not in CC_PROVIDERS:
                continue
            for cpu in ARCHES:
                for ref in doc.labels(attrs.get('srcs'), cpu):
                    if ref in self.regular_outputs:
                        self.schema_owners[self.regular_outputs[ref]['schema'], cpu].add(target)

    @cache
    def regular_schema(self, ref):
        if ref.startswith('@clickhouse//:') and ref in self.sources.inventory:
            filename = ref.removeprefix('@clickhouse//:')
            data = git_file(self.root, self.revision, filename)
            if self.working_tree and (self.root / filename).read_bytes() != data:
                raise ValueError('Root schema changed since ' + self.revision + ': ' + filename)
            return data.decode()
        return self.regular_schemas.get(ref)

    def resolve_schema(self, operand, directories, inputs):
        """Require the first protoc -I match, including unknown body shadows."""
        for directory in directories:
            physical = posixpath.normpath(posixpath.join(directory, operand))
            candidates = self.physical_schemas.get(physical)
            if candidates:
                if len(candidates) != 1:
                    return None
                ref = next(iter(candidates))
                if ref not in inputs or self.regular_schema(ref) is None:
                    return None
                return ref
        return None

    def regular_generator(self, name, cpu, attrs, outs):
        tools = set(strings(attrs.get('tools'), cpu))
        cmd = string(attrs.get('cmd'))
        if cmd is None or tools not in ({PROTOC}, {PROTOC, GRPC_PLUGIN}):
            return None
        cc = [out for out in outs if out.endswith('.pb.cc') and not out.endswith('.grpc.pb.cc')]
        if len(cc) != 1 or not cc[0].startswith('gen/' + cpu + '/'):
            return None
        stem = cc[0].removesuffix('.pb.cc')
        grpc = GRPC_PLUGIN in tools
        suffixes = ('.pb.h', '.pb.cc', '.grpc.pb.h', '.grpc.pb.cc') if grpc else ('.pb.h', '.pb.cc')
        if set(outs) != {stem + suffix for suffix in suffixes}:
            return None

        cmd = cmd.replace('gen/{arch}/', 'gen/' + cpu + '/')
        executable = '"$$execroot/$(execpath ' + PROTOC + ')" '
        before, separator, arguments = cmd.partition(executable)
        output_dir = posixpath.dirname(cc[0])
        mkdir = 'execroot=$$PWD\nmkdir -p $(BINDIR)/bazel/' + output_dir + '\n'
        base = '$$execroot/$(BINDIR)/bazel/'
        workdir_start = mkdir + 'mkdir -p "' + base
        if not separator or not before.startswith(workdir_start):
            return None
        workdir = before.removeprefix(workdir_start).partition('"')[0]
        if (not workdir.startswith('gen/' + cpu + '/') or
                not re.fullmatch(r'[A-Za-z0-9_+./-]+', workdir) or
                posixpath.normpath(workdir) != workdir):
            return None
        setup = workdir_start + workdir + '" && cd "' + base + workdir + '" && '
        if before not in {setup, setup + 'mkdir -p "' + base + output_dir + '" && '}:
            return None
        try:
            argv = shlex.split(arguments)
        except ValueError:
            return None
        if not argv or not argv[-1].startswith('$$execroot/'):
            return None
        physical = argv[-1].removeprefix('$$execroot/')
        refs = self.physical_schemas.get(physical)
        inputs = {label(ref, '@clickhouse') for ref in strings(attrs.get('srcs'), cpu)}
        if not refs or len(refs) != 1:
            return None
        source = next(iter(refs))
        if source not in inputs or self.regular_schema(source) is None:
            return None

        directories, options = [], {}
        args = iter(argv[:-1])
        for arg in args:
            if arg == '-I':
                directory = next(args, '')
                if (not directory.startswith('$$execroot/') or
                        posixpath.normpath(directory.removeprefix('$$execroot/')) != directory.removeprefix('$$execroot/')):
                    return None
                directories.append(directory.removeprefix('$$execroot/'))
            else:
                flag, equals, value = arg.partition('=')
                if flag not in {'--cpp_out', '--grpc_out', '--plugin'} or flag in options:
                    return None
                options[flag] = value if equals else next(args, '')
        cpp_out = options.get('--cpp_out', '')
        expected_options = {'--cpp_out', '--grpc_out', '--plugin'} if grpc else {'--cpp_out'}
        if (set(options) != expected_options or not cpp_out.startswith(base + 'gen/' + cpu + '/')
                or (grpc and (options['--grpc_out'] != cpp_out or
                              options['--plugin'] != 'protoc-gen-grpc=$$execroot/$(execpath ' + GRPC_PLUGIN + ')'))):
            return None
        virtual = next((physical[len(directory) + 1:] for directory in directories
                        if physical.startswith(directory + '/')), None)
        if (virtual is None or self.resolve_schema(virtual, directories, inputs) != source
                or posixpath.join(cpp_out.removeprefix(base), virtual.removesuffix('.proto')) != stem):
            return None

        pending, imports = [source], set()
        while pending:
            schema = pending.pop()
            if schema in imports:
                continue
            imports.add(schema)
            for operand in PROTO_IMPORT.findall(self.regular_schema(schema)):
                imported = self.resolve_schema(operand, directories, inputs)
                if imported is None:
                    return None
                pending.append(imported)
        return dict(producer='@clickhouse//bazel:' + name, schema=source, cpu=cpu,
                    outputs=tuple('@clickhouse//bazel:' + out for out in outs),
                    imports=tuple(sorted(imports - {source})),
                    runtimes=(PROTOBUF, GRPC) if grpc else (PROTOBUF,))

    @cache
    def classification(self, target, cpu):
        if target not in self.graph.rules:
            return None
        doc, kind, attrs = self.graph.rules[target]
        if doc.repo == GCP:
            return super().classification(target, cpu)
        if kind not in CC_COMPILERS:
            return None
        generated = {
            ref for ref in doc.labels(attrs.get('srcs'), cpu)
            if ref.endswith(TUS) and ref not in self.sources.directives
        }
        if not generated or any(ref not in self.regular_outputs for ref in generated):
            return None
        records = [self.regular_outputs[ref] for ref in sorted(generated)]
        own = {record['schema'] for record in records}
        dependencies, unresolved = set(), set()
        protobuf_headers = (self.graph.own_headers(PROTOBUF, cpu) |
                            self.graph.visible(PROTOBUF, cpu, public_only=True))
        for record in records:
            dependencies.update(record['runtimes'])
            for schema in record['imports']:
                if schema in own:
                    continue
                if (schema.startswith('@clickhouse_google_protobuf//:src/google/protobuf/')
                        and schema.removesuffix('.proto') + '.pb.h' in protobuf_headers):
                    continue
                owners = self.schema_owners.get((schema, cpu), set()) - {target}
                if len(owners) == 1:
                    dependencies.update(owners)
                else:
                    unresolved.add(schema)
        return dict(generated_sources=tuple(sorted(generated)), schemas=tuple(sorted(own)),
                    required_public_deps=tuple(sorted(dependencies)),
                    imports_without_single_native_owner=tuple(sorted(unresolved)),
                    produced_headers=tuple(sorted({out for record in records for out in record['outputs']
                                                   if out.endswith('.h')})))


class NativeTextual(Rewrite):
    def __init__(self, doc, graph, protos=None):
        super().__init__(doc, graph)
        self.decisions = {}
        self.protos = protos

    @cache
    def evidence(self, target, cpu):
        doc, _, attrs = self.graph.rules[target]
        own_inputs = set().union(*(doc.labels(attrs.get(field), cpu)
                                   for field in ('srcs', 'hdrs', 'textual_hdrs')))
        original = doc.labels(attrs.get('srcs'), cpu)
        generated = {ref for ref in original if ref in self.graph.rules or (
            ref.endswith(TUS) and ref not in self.graph.sources.directives)}
        public, pub_computed = self.graph.used(target, cpu, True)
        private, priv_computed = self.graph.used(target, cpu, False)
        used, computed = public | private, pub_computed | priv_computed
        if generated and self.protos is not None:
            proof = self.protos.classification(target, cpu)
            if proof is not None:
                runtime = self.protos.public_runtime_sources(target, cpu)
                if runtime is not None:
                    # The exact protoc/plugin commands and all schema imports
                    # have been verified. Keep real runtime header inclusions
                    # and any generated file outside that verified output set.
                    runtime_used, runtime_computed = runtime
                    used |= runtime_used
                    computed |= runtime_computed
                    generated.difference_update(proof['generated_sources'])
        return own_inputs | self.graph.visible(target, cpu), used, computed, generated

    def covered(self, value, target, arches, field):
        if field != 'additional_compiler_inputs' or not arches or target not in self.graph.rules:
            return False
        candidate = label(value, self.doc.repo)
        repo, sep, filename = candidate.partition('//:')
        sources = self.graph.sources
        if (not sep or repo in SPECIAL_REPOS or repo == self.doc.repo
                or repo not in sources.scanned or candidate not in sources.inventory
                or candidate in self.graph.rules or filename.endswith(CC_HEADER_SUFFIXES)):
            return False
        for cpu in arches:
            own_and_inherited, used, computed, generated = self.evidence(target, cpu)
            if candidate in own_and_inherited:
                reason = 'same_file_in_ccinfo_or_native_inputs'
            elif candidate in used:
                reason = 'native_include'
            elif generated:
                reason = 'unknown_generated_tu'
            elif repo in computed:
                reason = 'computed_include_in_owner'
            else:
                reason = 'no_native_include'
            self.decisions[target, cpu, candidate] = reason
            if reason not in {'same_file_in_ccinfo_or_native_inputs', 'no_native_include'}:
                return False
        return True


def expanded(node, document, stack=()):
    """Freeze semantic ASTs after resolving list constants, including globs."""
    if isinstance(node, ast.Name) and node.id in document.constants:
        if node.id in stack:
            raise ValueError('cyclic constant ' + node.id)
        return expanded(document.constants[node.id], document, (*stack, node.id))
    if isinstance(node, ast.AST):
        return type(node).__name__, tuple((key, expanded(value, document, stack))
                                          for key, value in ast.iter_fields(node))
    if isinstance(node, list):
        return tuple(expanded(value, document, stack) for value in node)
    return node


def unchanged_fields(before, after, graph):
    """Check all rule fields except native extra inputs, on both CPUs.

    Also check expanded Starlark expressions: unknown globs and select settings
    do not disappear merely because Document.values cannot evaluate them.
    """
    old_rules = [(call, {kw.arg: kw.value for kw in call.keywords})
                 for _, call in before.source.rules()]
    new_rules = [(call, {kw.arg: kw.value for kw in call.keywords})
                 for _, call in after.source.rules()]
    if len(old_rules) != len(new_rules):
        raise ValueError(str(before.path) + ': changed rules')
    checked = 0
    for (old, old_attrs), (new, new_attrs) in zip(old_rules, new_rules, strict=True):
        kind = getattr(old.func, 'id', '')
        if kind != getattr(new.func, 'id', '') or old_attrs.keys() != new_attrs.keys():
            raise ValueError(str(before.path) + ': changed rule fields')
        for field, original in old_attrs.items():
            if kind in CC_COMPILERS and field == 'additional_compiler_inputs':
                continue
            if expanded(original, before) != expanded(new_attrs[field], after):
                raise ValueError(f'{before.path}:{old.lineno}: changed {field}')
        if kind in CC_COMPILERS:
            for cpu in ARCHES:
                old_srcs = before.values(old_attrs.get('srcs'), cpu)
                new_srcs = after.values(new_attrs.get('srcs'), cpu)
                if old_srcs != new_srcs:
                    raise ValueError(f'{before.path}:{old.lineno}: changed ordered {cpu} srcs')
                checked += 1
    return checked


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--receipts', required=True, type=Path)
    parser.add_argument('--exclude', action='append', default=[],
                        help='leave this apparent repo unedited, e.g. @clickhouse_arrow')
    parser.add_argument('--consumer', action='append', default=[],
                        help='restrict changed consumers to this apparent repo, e.g. @clickhouse_orc')
    parser.add_argument('--report', type=Path)
    parser.add_argument('--revision', help='audit BUILD files at a Git revision; incompatible with --write')
    parser.add_argument('--llvm-archive', type=Path,
                        help='pinned @llvm module archive for C++ header ownership')
    parser.add_argument('--llvm-registry', type=Path, help='@llvm source.json from MODULE.bazel.lock')
    parser.add_argument('--llvm-inventory', type=Path, help='SHA-checked ClickHouse LLVM source inventory')
    parser.add_argument('--proto-archive', action='append', default=[], metavar='CONTRIB_OWNER=ARCHIVE',
                        help='pinned source archive for regular protoc schema/import verification')
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    if args.revision and args.write:
        parser.error('--revision is incompatible with --write')
    if bool(args.llvm_archive) != bool(args.llvm_registry and args.llvm_inventory):
        parser.error('--llvm-archive, --llvm-registry, and --llvm-inventory must be specified together')
    sources = Sources(args.receipts, args.root)
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
    cpp_headers = libcxx_headers(args.root, args.llvm_archive, args.llvm_registry, args.llvm_inventory)
    graph = TextualGraph(documents, sources, cpp_headers)
    protos = NativeProtos(args.root, args.revision or 'HEAD', graph, sources, args.proto_archive,
                          working_tree=args.revision is None)
    changes, reasons = Counter(), Counter()
    removed, retained = defaultdict(set), defaultdict(set)
    for doc in documents:
        if doc.repo in SPECIAL_REPOS | set(args.exclude) or args.consumer and doc.repo not in args.consumer:
            continue
        rewrite = NativeTextual(doc, graph, protos)
        result = rewrite.text()
        for (target, cpu, candidate), reason in rewrite.decisions.items():
            reasons[reason] += 1
            destination = removed if reason in {
                'same_file_in_ccinfo_or_native_inputs', 'no_native_include'
            } else retained
            destination[candidate].add((target, cpu, reason))
        if result != doc.source.bytes.decode():
            after = Document(doc.path, sources.inventories, text=result)
            changes['ordered_srcs_and_fields_verified'] += unchanged_fields(doc, after, graph)
            changes['additional_compiler_input_literals'] += rewrite.stats['inherited_source_header_literals']
            changes['files'] += 1
            print(doc.path.relative_to(args.root), rewrite.stats['inherited_source_header_literals'])
            if args.write:
                if doc.path.read_text() != doc.source.bytes.decode():
                    raise ValueError('BUILD file changed during audit: ' + str(doc.path))
                doc.path.write_text(result)
    print('Results:', dict(changes), 'written' if args.write else 'no files written')
    print('Decisions (per native target/CPU/file):', dict(reasons))
    if args.report:
        args.report.write_text(json.dumps({
            'changes': dict(changes),
            'proved_regular_proto_outputs': len(protos.regular_outputs),
            'reason_count': dict(reasons),
            'eligible': {k: sorted(v) for k, v in sorted(removed.items())},
            'retain': {k: sorted(v) for k, v in sorted(retained.items())},
        }, indent=2) + '\n')


if __name__ == '__main__':
    main()

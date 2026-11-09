#!/usr/bin/env python3
"""Account for Google Cloud protobuf C++ sources using their pinned schemas.

The CODEGEN_RULES commands must use this repository's protoc and grpc plugin.
An arbitrary generated .cc file is never classified by its suffix alone.
Public include requirements come from the declared imports and the protobuf
and gRPC libraries, not from the copied CMake includes on each proto target.
"""

import argparse
import ast
from collections import Counter, defaultdict
from functools import cache
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.codegen import GCP_OUTPUT, GOOGLEAPIS_ARCHIVE, GOOGLEAPIS_OUTPUT, parse_google_api_schemas
from initial_migration.native_cc_inputs import Sources, TUS
from initial_migration.prune_inherited_headers import ARCHES, CC_COMPILERS, CC_PROVIDERS
from initial_migration.repository_inputs import ROOT

GCP = '@clickhouse_google_cloud_cpp'
PROTOBUF = '@clickhouse_google_protobuf//:_libprotobuf'
GRPC = '@clickhouse_grpc//:grpc++'
PROTOC = '@clickhouse_google_protobuf//:protoc'
GRPC_PLUGIN = '@clickhouse_grpc//:grpc_cpp_plugin'
GENERATED = GCP_OUTPUT
PROTO_IMPORT = re.compile(r'^\s*import\s+(?:public\s+|weak\s+)?["\']([^"\']+)["\']\s*;',
                          re.MULTILINE)


def git_file(root, revision, filename):
    return subprocess.check_output(['git', 'show', revision + ':' + filename], cwd=root)

def codegen_rules_filename(root, revision):
    """Locate CODEGEN_RULES in either the current branch or a recorded proof."""
    filename = 'bazel/rules/codegen_rules.bzl'
    check = subprocess.run(['git', 'cat-file', '-e', revision + ':' + filename],
                           cwd=root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if check.returncode:
        filename = 'bazel/initial_migration/codegen_rules.bzl'
    return filename


def string(node):
    """Evaluate only known generator string concatenations."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = string(node.left), string(node.right)
        if left is not None and right is not None:
            return left + right
    if (isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'repository_path'
            and len(node.args) == 1 and not node.keywords):
        return string(node.args[0])
    return None


def dictionary(node):
    if not isinstance(node, ast.Dict):
        return {}
    return {key.value: value for key, value in zip(node.keys, node.values, strict=True)
            if isinstance(key, ast.Constant) and isinstance(key.value, str)}


def strings(node, cpu):
    if not isinstance(node, (ast.List, ast.Tuple)):
        return ()
    result = []
    for item in node.elts:
        value = string(item)
        if value is None:
            return ()
        result.append(value.replace('gen/{arch}/', 'gen/' + cpu + '/'))
    return tuple(result)


def declared_codegen_outputs(name, node, cpu, read_file):
    """Read the output names of an entry in CODEGEN_RULES."""
    if (name == 'extract_googleapis' and isinstance(node, ast.Call)
            and getattr(node.func, 'id', None) == 'googleapis_archive_inputs'):
        schemas = parse_google_api_schemas(read_file('bazel/rules/googleapis.bzl'))
        return 'genrule', tuple('gen/' + GOOGLEAPIS_OUTPUT + schema for schema in schemas)
    attrs = dictionary(node)
    if not attrs:
        return None, ()
    return string(attrs.get('kind')) or 'genrule', strings(attrs.get('outs'), cpu)


class GoogleProtos:
    def __init__(self, root, revision, graph, sources):
        self.graph, self.sources = graph, sources
        self.schemas = {}
        self.outputs = {}
        self.skipped = Counter()
        self.schema_owners = defaultdict(set)
        data = git_file(root, revision, GOOGLEAPIS_ARCHIVE)
        with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
            for member in archive.getmembers():
                if member.isfile() and member.name.endswith('.proto'):
                    self.schemas[member.name.split('/', 1)[1]] = (
                        archive.extractfile(member).read().decode())

        source = git_file(root, revision, codegen_rules_filename(root, revision)).decode()
        tree = ast.parse(source)
        constants = {
            statement.targets[0].id: statement.value for statement in tree.body
            if isinstance(statement, ast.Assign) and len(statement.targets) == 1
            and isinstance(statement.targets[0], ast.Name)
        }
        shared = dictionary(constants.get('_SHARED_CODEGEN'))
        for name, rule in dictionary(constants.get('CODEGEN_RULES')).items():
            if isinstance(rule, ast.Dict):
                # New Google APIs rules own all four outputs, for either CPU.
                cpu = None
                attrs = dictionary(rule)
                outs = strings(attrs.get('outs'), '')
                prefix = 'gen/' + GENERATED
            elif (isinstance(rule, ast.Call)
                  and getattr(rule.func, 'id', '') == 'codegen_for_arch'
                  and len(rule.args) == 2 and isinstance(rule.args[0], ast.Subscript)):
                template = string(rule.args[0].slice)
                cpu = string(rule.args[1])
                if cpu not in ARCHES or template not in shared:
                    continue
                attrs = dictionary(shared[template])
                outs = strings(attrs.get('outs'), cpu)
                prefix = 'gen/' + cpu + '/' + GENERATED
            else:
                continue
            if not any(value.startswith(prefix) for value in outs):
                continue
            record = self.generator(name, cpu, attrs, outs)
            if record is None:
                continue
            for output in record['outputs']:
                self.outputs[output] = record
        for target, (doc, kind, attrs) in graph.rules.items():
            if doc.repo != GCP or kind not in CC_PROVIDERS:
                continue
            for cpu in ARCHES:
                for ref in doc.labels(attrs.get('srcs'), cpu):
                    if ref in self.outputs:
                        record = self.outputs[ref]
                        self.schema_owners[record['schema'], cpu].add(target)

    def generator(self, name, cpu, attrs, outs):
        """Require exact compiler, plugin, inputs, outputs, and pinned schema."""
        tools = set(strings(attrs.get('tools'), cpu or ''))
        cmd = string(attrs.get('cmd'))
        if cmd is None or tools not in ({PROTOC}, {PROTOC, GRPC_PLUGIN}):
            self.skipped['unknown_generator'] += 1
            return None
        root = 'gen/' + (cpu + '/' if cpu else '')
        cmd = cmd.replace('gen/{arch}/', root)
        prefix = root + GENERATED
        names = [path.removeprefix(prefix) for path in outs]
        if any(path == name for path, name in zip(outs, names, strict=True)):
            self.skipped['foreign_output'] += 1
            return None
        first = names[0]
        grpc = first.endswith('.grpc.pb.cc') or cpu is None
        suffix = '.grpc.pb.cc' if first.endswith('.grpc.pb.cc') else '.pb.cc'
        if not first.endswith(suffix):
            self.skipped['not_proto_cc'] += 1
            return None
        schema = first.removesuffix(suffix) + '.proto'
        stem = schema.removesuffix('.proto')
        expected = {stem + ('.grpc.pb.cc' if grpc else '.pb.cc'),
                    stem + ('.grpc.pb.h' if grpc else '.pb.h')}
        if cpu is None:
            expected.update((stem + '.pb.cc', stem + '.pb.h'))
        srcs = strings(attrs.get('srcs'), cpu or '')
        source = '//bazel:' + root + GOOGLEAPIS_OUTPUT + schema
        if (set(names) != expected or schema not in self.schemas or source not in srcs
                or '$(execpath ' + PROTOC + ')' not in cmd
                or '--cpp_out' not in cmd or GOOGLEAPIS_OUTPUT + schema not in cmd
                or (grpc and (GRPC_PLUGIN not in tools or '--grpc_out' not in cmd
                             or '$(execpath ' + GRPC_PLUGIN + ')' not in cmd))
                or (not grpc and GRPC_PLUGIN in tools)):
            self.skipped['unproved_command_or_schema'] += 1
            return None
        transitive = self.imports(schema)
        if transitive['missing']:
            self.skipped['unresolved_schema_imports'] += 1
            return None
        return dict(producer='@clickhouse//bazel:' + name, schema=schema, cpu=cpu, grpc=grpc,
                    outputs=tuple('@clickhouse//bazel:' + output for output in outs),
                    direct_imports=tuple(PROTO_IMPORT.findall(self.schemas[schema])),
                    imports=transitive, runtimes=(PROTOBUF, GRPC) if grpc else (PROTOBUF,))

    @cache
    def imports(self, schema):
        pending = [schema]
        googleapis, protobuf, missing = set(), set(), set()
        while pending:
            current = pending.pop()
            if current in googleapis or current in protobuf or current in missing:
                continue
            if current in self.schemas:
                googleapis.add(current)
                pending.extend(PROTO_IMPORT.findall(self.schemas[current]))
            elif '@clickhouse_google_protobuf//:src/' + current in self.sources.inventory:
                # Standard protobuf dependencies are published by libprotobuf.
                protobuf.add(current)
            else:
                missing.add(current)
        return dict(googleapis=tuple(sorted(googleapis)), protobuf=tuple(sorted(protobuf)),
                    missing=tuple(sorted(missing)))

    @cache
    def classification(self, target, cpu):
        if target not in self.graph.rules:
            return None
        doc, kind, attrs = self.graph.rules[target]
        if doc.repo != GCP or kind not in CC_COMPILERS:
            return None
        srcs = doc.labels(attrs.get('srcs'), cpu)
        generated = {ref for ref in srcs if ref.endswith(TUS) and ref not in self.sources.directives}
        if not generated or any(ref not in self.outputs for ref in generated):
            return None
        records = [self.outputs[ref] for ref in sorted(generated)]
        own_schemas = {r['schema'] for r in records}
        dependencies = set()
        unresolved = set()
        for record in records:
            dependencies.update(record['runtimes'])
            for schema in record['direct_imports']:
                if schema in own_schemas or schema in record['imports']['protobuf']:
                    continue
                owners = self.schema_owners.get((schema, cpu), set()) - {target}
                if len(owners) == 1:
                    dependencies.update(owners)
                else:
                    unresolved.add(schema)
        return dict(generated_sources=tuple(sorted(generated)), schemas=tuple(sorted(own_schemas)),
                    required_public_deps=tuple(sorted(dependencies)),
                    imports_without_single_native_owner=tuple(sorted(unresolved)),
                    produced_headers=tuple(sorted({output for r in records for output in r['outputs']
                                                   if output.endswith('.h')})))

    @cache
    def public_runtime_sources(self, target, cpu):
        proof = self.classification(target, cpu)
        if proof is None or proof['imports_without_single_native_owner']:
            return None
        runtime = set()
        computed = set()
        for rule in (PROTOBUF, GRPC):
            if rule not in proof['required_public_deps']:
                continue
            files, macros = self.graph.used(rule, cpu, True)
            runtime.update(files)
            computed.update(macros)
        return frozenset(runtime), frozenset(computed)

    def report(self):
        consumers = []
        for target, (doc, kind, _) in self.graph.rules.items():
            if doc.repo != GCP or kind not in CC_COMPILERS:
                continue
            for cpu in ARCHES:
                proof = self.classification(target, cpu)
                if proof:
                    consumers.append(dict(target=target, cpu=cpu, **proof))
        return dict(proved_outputs=len(self.outputs), skipped=dict(self.skipped), consumers=consumers,
                    schemas=len(self.schemas))


def main():
    from initial_migration.native_cc_dependencies import DependencyGraph
    from initial_migration.prune_inherited_headers import Document

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--receipts', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    sources = Sources(args.receipts, args.root)
    sources.require_complete()
    files = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', args.revision, '--',
                                     'BUILD.bazel', 'bazel/dependencies'], cwd=args.root).decode().splitlines()
    docs = [Document(args.root / path, sources.inventories, git_file(args.root, args.revision, path).decode())
            for path in files if path == 'BUILD.bazel' or path.endswith('.BUILD.bazel')]
    graph = DependencyGraph(docs, sources)
    model = GoogleProtos(args.root, args.revision, graph, sources)
    result = dict(revision=args.revision, **model.report())
    args.report.write_text(json.dumps(result, indent=2) + '\n')
    print('Google API schemas:', result['schemas'], 'proved generated outputs:', result['proved_outputs'],
          'target/arch pairs:', len(result['consumers']), 'skipped:', result['skipped'])


if __name__ == '__main__':
    main()

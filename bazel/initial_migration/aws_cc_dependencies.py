#!/usr/bin/env python3
"""Use AWS CRT include evidence to replace the copied CMake include graph.

Pass --facts with the scan_include_adjacency.py v3 directory. This updates
only the AWS source overlays. Other BUILD files retain their exports until
their direct source references have also been migrated.
"""

import argparse
import ast
from collections import defaultdict
import copy
import gzip
import json
from pathlib import Path, PurePosixPath
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.compact_build import Source, remove_unused_constants, simplify_list_expressions
from initial_migration.prune_inherited_headers import ARCHES, Document, label
from initial_migration.repository_inputs import CC_HEADER_SUFFIXES, REPOSITORIES, ROOT

COMPILED = ('.c', '.cc', '.cpp', '.cxx', '.c++', '.C', '.s', '.S', '.asm')
CONFIG = '@clickhouse_aws//:aws_config_headers'
CONFIG_HEADERS = (
    'contrib/aws-cmake/include/aws/common/config.h',
    'contrib/aws-cmake/include/aws/core/SDKConfig.h',
    'contrib/aws-cmake/include/aws/crt/Config.h',
)
CORE_INCLUDES = (
    'src/aws-cpp-sdk-core/include',
    'generated/src/aws-cpp-sdk-s3/include',
    'generated/src/aws-cpp-sdk-glue/include',
)


def header_target(doc):
    name = doc.repo.removeprefix('@clickhouse_')
    return doc.repo + '//:' + (name if name == 'aws_c_mqtt' else name + '_headers')


def includes(doc):
    return CORE_INCLUDES if doc.repo == '@clickhouse_aws' else ('include',)


def strings(values):
    return ast.List(elts=[ast.Constant(value=x) for x in values], ctx=ast.Load())


def add(left, right):
    if not right:
        return left
    if left is None or isinstance(left, ast.List) and not left.elts:
        return strings(right)
    if isinstance(left, ast.List):
        return ast.List(elts=[*left.elts, *strings(right).elts], ctx=ast.Load())
    return ast.BinOp(left=left, op=ast.Add(), right=strings(right))


def compiled(doc, attrs, arch):
    return [value for value in doc.values(attrs.get('srcs'), arch)
            if value.endswith(COMPILED)]


class AwsMigration:
    def __init__(self, facts_directory):
        self.facts = {}
        inventories = {}
        for owner, spec in REPOSITORIES.items():
            if not spec['name'].startswith('clickhouse_aws'):
                continue
            path = facts_directory / (spec['name'] + '-v3-' + spec['sha256'] + '.json.gz')
            with gzip.open(path, 'rt') as stream:
                facts = json.load(stream)
            if (facts['sha256'], facts['commit'], facts['version']) != (spec['sha256'], spec['commit'], 3):
                raise ValueError('Unverified AWS archive ' + str(path))
            inventories[owner] = facts['inventory']
            self.facts['@' + spec['name']] = facts
        self.docs = {
            doc.repo: doc for doc in (
                Document(path, inventories)
                for path in sorted((ROOT / 'bazel/dependencies').glob('clickhouse_aws*.BUILD.bazel'))
            )
        }
        self.names = {}
        self.headers = {}
        self.original_sources = {}
        self.index = defaultdict(list)
        for doc in self.docs.values():
            for root in includes(doc):
                for file in doc.files:
                    if file.startswith(root + '/'):
                        self.index[file.removeprefix(root + '/')].append((doc.repo, file))
            for _, call in doc.source.rules():
                attrs = {kw.arg: kw.value for kw in call.keywords}
                name = ast.literal_eval(attrs['name'])
                target = label(':' + name, doc.repo)
                self.names[target] = (call.func.id, doc, attrs)
                if call.func.id == 'ch_cc_library':
                    for arch in ARCHES:
                        self.original_sources[target, arch] = compiled(doc, attrs, arch)
                if target == header_target(doc):
                    self.headers[doc.repo] = attrs
        if set(self.headers) != set(self.docs):
            raise ValueError('Every AWS overlay must have an existing source-header provider')
        self.public_deps = {}
        for doc in self.docs.values():
            filenames = {v.removeprefix(':') for v in doc.values(self.headers[doc.repo].get('hdrs'), 'x86_64')}
            deps, _ = self.references(doc, filenames)
            if doc.repo == '@clickhouse_aws_c_common':
                deps.add(CONFIG)
            # SDK and CRT headers require their generated configuration. The
            # independent configuration target has no SDK dependencies.
            if doc.repo in {'@clickhouse_aws', '@clickhouse_aws_crt_cpp'}:
                deps.add(CONFIG)
            self.public_deps[header_target(doc)] = deps
        self.assert_header_dag()

    def references(self, doc, filenames):
        """Find direct foreign header requirements and own textual includes."""
        deps = set()
        textual = set()
        facts = self.facts[doc.repo]
        for filename in sorted(filenames):
            for line, directive, mode, operand in facts['files'].get(filename, ()):
                if directive not in {'#include', '#include_next', '#import', '#embed'}:
                    continue
                if mode == 'computed':
                    raise ValueError(f'Unresolved {doc.repo}//:{filename}:{line}: {operand}')
                if mode not in {'quote', 'angle'}:
                    continue
                if mode == 'quote':
                    relative = str(PurePosixPath(filename).parent / operand)
                    if relative in doc.files:
                        if not relative.endswith(CC_HEADER_SUFFIXES):
                            textual.add(relative)
                        continue
                matches = self.index.get(operand, [])
                for owner, source in matches:
                    if owner == doc.repo:
                        if not source.endswith(CC_HEADER_SUFFIXES):
                            textual.add(source)
                    else:
                        deps.add(header_target(self.docs[owner]))
                if operand.startswith('openssl/'):
                    deps.add('@clickhouse_openssl//:crypto')
        return deps, textual

    def assert_header_dag(self):
        done = set()
        def visit(target, active):
            if target in done:
                return
            if target in active:
                raise ValueError('AWS public-header dependency cycle: ' + ' -> '.join((*active, target)))
            for dep in sorted(self.public_deps.get(target, ())):
                visit(dep, (*active, target))
            done.add(target)
        for target in self.public_deps:
            visit(target, ())

    def closure(self, target):
        todo = [target]
        seen = set()
        while todo:
            node = todo.pop()
            if node in seen:
                continue
            seen.add(node)
            todo.extend(self.public_deps.get(node, ()))
        return seen

    def expr(self, doc, node, keep, names=()):
        """Filter label lists without changing select conditions or glob order."""
        if node is None:
            return strings([])
        if isinstance(node, ast.Name) and node.id in doc.constants:
            if node.id in names:
                raise ValueError('Recursive BUILD constant ' + node.id)
            return self.expr(doc, doc.constants[node.id], keep, (*names, node.id))
        if isinstance(node, (ast.List, ast.Tuple)):
            return ast.List(elts=[
                copy.deepcopy(item) for item in node.elts
                if not isinstance(item, ast.Constant) or not isinstance(item.value, str) or keep(item.value)
            ], ctx=ast.Load())
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return ast.BinOp(left=self.expr(doc, node.left, keep, names), op=ast.Add(),
                             right=self.expr(doc, node.right, keep, names))
        if (isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'select'
                and len(node.args) == 1 and isinstance(node.args[0], ast.Dict)):
            table = node.args[0]
            return ast.Call(func=ast.Name(id='select', ctx=ast.Load()),
                            args=[ast.Dict(keys=copy.deepcopy(table.keys),
                                           values=[self.expr(doc, v, keep, names) for v in table.values])],
                            keywords=[])
        if isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'glob':
            # Only remove a glob when all of its files would be removed.
            values = set().union(*(set(doc.values(node, arch)) for arch in ARCHES))
            if values and not any(keep(value) for value in values):
                return strings([])
        return copy.deepcopy(node)

    def retained_input(self, doc, value, field, needed_text):
        canonical = label(value, doc.repo)
        repo, separator, filename = canonical.partition('//:')
        if not separator:
            return True
        if filename.startswith(('gen/', 'config/')):
            if repo == '@clickhouse_aws' and any(filename.endswith(x) for x in CONFIG_HEADERS):
                return False
            return True
        if repo in self.docs:
            if repo != doc.repo:
                return False
            if any(filename.startswith(root + '/') for root in includes(doc)):
                return False
            if field == 'additional_compiler_inputs':
                return filename in needed_text
            return True
        if canonical.startswith('@clickhouse//:contrib/openssl-cmake/common/include/'):
            return False
        if repo == '@clickhouse_openssl':
            return False
        return True

    def local_includes(self, doc, node):
        def own_include(item):
            if isinstance(item, ast.Constant) and isinstance(item.value, str):
                if item.value in includes(doc) or (doc.repo == '@clickhouse_aws' and item.value in {
                        'config/' + arch + '/contrib/aws-cmake/include' for arch in ARCHES}):
                    return None
                return item
            if (isinstance(item, ast.Call) and getattr(item.func, 'id', '') == 'repository_local_include'):
                # Foreign include directories belonged to the original single
                # CMake SDK compile group. The AWS CcInfo providers supply the
                # directories required by each physical source compilation.
                directory = item.args[0]
                root = directory.left if isinstance(directory, ast.BinOp) and isinstance(directory.op, ast.Add) else directory
                if (isinstance(root, ast.Call) and getattr(root.func, 'id', '') == 'repository_path'
                        and len(root.args) == 1 and isinstance(root.args[0], ast.Constant)
                        and root.args[0].value == doc.owner):
                    if root is directory:
                        path = '.'
                    elif isinstance(directory.right, ast.Constant) and isinstance(directory.right.value, str):
                        path = directory.right.value.lstrip('/')
                    else:
                        raise ValueError('Unexpected AWS local include: ' + ast.unparse(item))
                    return ast.Constant(value=path) if path not in includes(doc) else None
                return None
            return item
        def walk(n):
            if isinstance(n, ast.Name) and n.id in doc.constants:
                return walk(doc.constants[n.id])
            if isinstance(n, (ast.List, ast.Tuple)):
                return ast.List(elts=[copy.deepcopy(value) for item in n.elts
                                      if (value := own_include(item)) is not None], ctx=ast.Load())
            if isinstance(n, ast.BinOp) and isinstance(n.op, ast.Add):
                return ast.BinOp(left=walk(n.left), op=ast.Add(), right=walk(n.right))
            if (isinstance(n, ast.Call) and getattr(n.func, 'id', '') == 'select'
                    and len(n.args) == 1 and isinstance(n.args[0], ast.Dict)):
                table = n.args[0]
                return ast.Call(func=ast.Name(id='select', ctx=ast.Load()),
                                args=[ast.Dict(keys=copy.deepcopy(table.keys), values=[walk(v) for v in table.values])],
                                keywords=[])
            return copy.deepcopy(n)
        return walk(node)

    def update(self, doc):
        edits = []
        for _, call in doc.source.rules():
            kind = call.func.id
            attrs = {kw.arg: kw.value for kw in call.keywords}
            target = label(':' + ast.literal_eval(attrs['name']), doc.repo)
            new = {}
            if target == header_target(doc):
                new['includes'] = strings(includes(doc))
                new['deps'] = strings(sorted(self.public_deps[target]))
                textual = set()
                for arch in ARCHES:
                    _, needed = self.references(
                        doc, {v.removeprefix(':') for v in doc.values(attrs.get('hdrs'), arch)})
                    textual.update(needed)
                # Files under an exported include directory are public even
                # when a .c includes a .def rather than a normal .h.
                for native_target, (native_kind, owner, native_attrs) in self.names.items():
                    if native_kind != 'ch_cc_library' or owner is not doc:
                        continue
                    srcs = {v.removeprefix(':') for arch in ARCHES
                            for v in compiled(doc, native_attrs, arch) if not v.startswith('@')}
                    _, needed = self.references(doc, srcs)
                    textual.update(n for n in needed if any(n.startswith(root + '/') for root in includes(doc)))
                if textual:
                    present = {v.removeprefix(':') for arch in ARCHES
                               for v in doc.values(attrs.get('textual_hdrs'), arch)}
                    new['textual_hdrs'] = add(attrs.get('textual_hdrs'), sorted(textual - present))
            elif kind == 'ch_cc_library':
                srcs = {v.removeprefix(':') for arch in ARCHES
                        for v in compiled(doc, attrs, arch) if not v.startswith('@')}
                required, needed_text = self.references(doc, srcs)
                # Own source headers can include outside the compile directory.
                local_headers = {
                    v.removeprefix(':') for arch in ARCHES
                    for v in doc.values(attrs.get('srcs'), arch)
                    if not v.startswith('@') and v.endswith(CC_HEADER_SUFFIXES)
                    and not any(v.removeprefix(':').startswith(root + '/') for root in includes(doc))
                }
                more, needed = self.references(doc, local_headers)
                required.update(more)
                needed_text.update(needed)
                provider = header_target(doc)
                covered = self.closure(provider)
                for key in ('srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs'):
                    if key in attrs:
                        new[key] = self.expr(doc, attrs[key], lambda v: self.retained_input(doc, v, key, needed_text))
                # Remove earlier copied AWS header deps, retaining all LLVM
                # and compiler dependencies. Reconstruct the true AWS deps
                # from this target's own sources and public headers.
                def other_dep(value):
                    return label(value, doc.repo).partition('//:')[0] not in self.docs
                new['deps'] = add(self.expr(doc, attrs.get('deps'), other_dep),
                                  [':' + provider.partition('//:')[2]])
                new['implementation_deps'] = add(
                    self.expr(doc, attrs.get('implementation_deps'), other_dep),
                    sorted(required - covered))
                if 'local_includes' in attrs:
                    new['local_includes'] = self.local_includes(doc, attrs['local_includes'])
            else:
                continue
            additions = []
            for key, value in new.items():
                value = ast.fix_missing_locations(value)
                text = ast.unparse(value)
                if key in attrs:
                    edits.append((*doc.source.span(attrs[key]), text))
                else:
                    additions.append('    ' + key + ' = ' + text + ',\n')
            if additions:
                pos = doc.source.span(call)[1] - 1
                edits.append((pos, pos, '\n' + ''.join(additions)))
        text = doc.source.rewrite(edits)
        if doc.repo == '@clickhouse_aws' and CONFIG not in self.names:
            hdrs = {f'@platforms//cpu:{arch}': ['config/' + arch + '/' + name for name in CONFIG_HEADERS]
                    for arch in ARCHES}
            roots = {f'@platforms//cpu:{arch}': ['config/' + arch + '/contrib/aws-cmake/include'] for arch in ARCHES}
            text += ('\ncc_library(\n    name = "aws_config_headers",\n'
                     + '    hdrs = select(' + repr(hdrs) + '),\n'
                     + '    includes = select(' + repr(roots) + '),\n)\n')
        # Remove only dead migration constants; preserve literal file order.
        while True:
            after = simplify_list_expressions(Source(remove_unused_constants(Source(text))))
            if after == text:
                break
            text = after
        # Omit empty include/dependency attributes and unused repository-path
        # imports. ch_cc_library and cc_library use the same empty defaults.
        source = Source(text)
        edits = []
        for _, call in source.rules():
            if call.func.id not in {'cc_library', 'ch_cc_library'}:
                continue
            for kw in call.keywords:
                if kw.arg in {'local_includes', 'deps', 'implementation_deps'} and isinstance(
                        kw.value, ast.List) and not kw.value.elts:
                    start, end = source.span(kw)
                    end += len(source.bytes[end:]) - len(source.bytes[end:].lstrip(b' \t'))
                    if source.bytes[end:end + 1] == b',':
                        end += 1
                    edits.append((start, end, ''))
        used_names = {n.id for n in ast.walk(source.module) if isinstance(n, ast.Name)}
        for statement in source.module.body:
            if (isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call)
                    and getattr(statement.value.func, 'id', '') == 'load'):
                call = statement.value
                if (len(call.args) > 1 and isinstance(call.args[0], ast.Constant)
                        and call.args[0].value == '@clickhouse//bazel/rules:repository_paths.bzl'
                        and not any(isinstance(n, ast.Constant) and n.value in used_names for n in call.args[1:])):
                    edits.append((*source.span(statement), ''))
        return source.rewrite(edits)

    def check(self, doc, text):
        check = object.__new__(Document)
        check.__dict__.update(doc.__dict__)
        check.source = Source(text)
        check.constants = {statement.targets[0].id: statement.value for statement in check.source.module.body
                           if isinstance(statement, ast.Assign) and len(statement.targets) == 1
                           and isinstance(statement.targets[0], ast.Name)}
        for _, call in check.source.rules():
            if call.func.id != 'ch_cc_library':
                continue
            attrs = {kw.arg: kw.value for kw in call.keywords}
            target = label(':' + ast.literal_eval(attrs['name']), doc.repo)
            for arch in ARCHES:
                actual = compiled(check, attrs, arch)
                if actual != self.original_sources[target, arch]:
                    raise ValueError(f'Changed {target} {arch} compiled source order')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--facts', type=Path, required=True)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    migration = AwsMigration(args.facts)
    updates = {}
    for doc in migration.docs.values():
        text = migration.update(doc)
        migration.check(doc, text)
        if text != doc.source.bytes.decode():
            updates[doc.path] = text
    for target, deps in sorted(migration.public_deps.items()):
        print(target, sorted(deps))
    for path, text in updates.items():
        print(path.relative_to(ROOT), len(text.splitlines()), 'lines')
        if args.write:
            path.write_text(text)


if __name__ == '__main__':
    main()

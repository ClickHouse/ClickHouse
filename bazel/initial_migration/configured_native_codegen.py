#!/usr/bin/env python3
"""Give paired native generators one configured output filename.

The migration runs after the original CMake graphs and packages are rendered.
It checks actions in both metadata modes and checks BUILD files on both CPUs
and an unsupported CPU. Only files emitted by complete generator pairs move.
"""
import argparse
import ast
from collections import Counter, defaultdict
import copy
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'bazel'))
from initial_migration.compact_build import Source, ast_key
from initial_migration.configured_codegen_outputs import (
    Migration as ExternalCodegen, Instantiate, data_assignments, dictionary,
)
from initial_migration.configured_external_outputs import CPUS, MapConstants, attributes, text_constant
from initial_migration import factor_build_selections as selections


def select_cpu(branches):
    return 'select({' + ''.join(
        json.dumps('@platforms//cpu:' + cpu) + ': ' + value + ','
        for cpu, value in branches.items()
    ) + '})'


class ConfiguredAction(ast.NodeTransformer):
    """Evaluate recorded build options and CMake mode, retaining tool expressions."""
    def __init__(self, migration, cpu, cmake):
        self.migration = migration
        self.cpu = cpu
        self.cmake = cmake

    def visit_Name(self, node):
        if node.id == 'CMAKE_METADATA':
            return copy.deepcopy(self.migration.metadata)
        if node.id == 'BUILD_OPTIONS':
            return copy.deepcopy(self.migration.options_node)
        return node

    def visit_Subscript(self, node):
        node = self.generic_visit(node)
        if isinstance(node.value, ast.Dict) and text_constant(node.slice) in dictionary(node.value):
            return self.visit(copy.deepcopy(dictionary(node.value)[text_constant(node.slice)]))
        return node

    def visit_Call(self, node):
        node = self.generic_visit(node)
        if (getattr(node.func, 'id', '') == 'select' and len(node.args) == 1
                and isinstance(node.args[0], ast.Dict)):
            choices = dictionary(node.args[0])
            for setting in (
                '//bazel:use_cmake_metadata_' + self.cpu if self.cmake else '',
                '//bazel:use_cmake_metadata' if self.cmake else '',
                '@platforms//cpu:' + self.cpu, '//conditions:default',
            ):
                if setting in choices:
                    return copy.deepcopy(choices[setting])
            return ast.Name(id='_NO_MATCH_', ctx=ast.Load())
        if (getattr(node.func, 'id', '') == 'str' and len(node.args) == 1
                and isinstance(node.args[0], ast.Constant)):
            return ast.Constant(str(node.args[0].value))
        if (isinstance(node.func, ast.Attribute) and node.func.attr == 'get'
                and isinstance(node.func.value, ast.Dict) and len(node.args) == 2):
            return copy.deepcopy(dictionary(node.func.value).get(text_constant(node.args[0]), node.args[1]))
        return node

    def visit_BinOp(self, node):
        node = self.generic_visit(node)
        if isinstance(node.op, ast.Add):
            if (isinstance(node.left, ast.Constant) and isinstance(node.left.value, str)
                    and isinstance(node.right, ast.Constant) and isinstance(node.right.value, str)):
                return ast.Constant(node.left.value + node.right.value)
            if isinstance(node.left, ast.List) and isinstance(node.right, ast.List):
                return ast.List(elts=node.left.elts + node.right.elts, ctx=ast.Load())
        return node


class BuildOptionBindings(ast.NodeTransformer):
    def __init__(self, migration, cpu):
        self.options = migration.options[cpu]
        self.cpu = cpu

    def visit_Name(self, node):
        if node.id == 'arch':
            return ast.Constant(self.cpu)
        if node.id == 'attrs':
            return copy.deepcopy(self.options)
        return node


class Migration(ExternalCodegen):
    def __init__(self, root):
        super().__init__(root)
        self.eligible = {}
        self.facts = {}

    def collect(self, source):
        self.definitions = data_assignments(source)
        raw = dictionary(self.definitions['CODEGEN_RULES'])
        templates = dictionary(self.definitions.get('_SHARED_CODEGEN', ast.Dict(keys=[], values=[])))
        self.options_node = data_assignments(Source(
            (self.root / 'bazel/rules/build_options_data.bzl').read_text()))['BUILD_OPTIONS']
        self.options = dictionary(self.options_node)
        self.metadata = data_assignments(Source(
            (self.root / 'bazel/initial_migration/cmake_metadata.bzl').read_text()))['CMAKE_METADATA']
        families = defaultdict(dict)
        for name, node in raw.items():
            match = re.fullmatch(r'generate_(x86_64|aarch64)_(.+)', name)
            if not match:
                continue
            cpu, family = match.groups()
            if isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'codegen_for_arch':
                node = Instantiate(cpu).visit(copy.deepcopy(templates[text_constant(node.args[0].slice)]))
            elif not isinstance(node, ast.Dict):
                raise ValueError(('Unknown original generator', name, ast.dump(node)))
            families[family][cpu] = name, node
        self.original = raw
        for family, pair in families.items():
            if set(pair) != set(CPUS):
                continue
            expected = None
            for cpu, (name, node) in pair.items():
                outputs = tuple(text_constant(elt) for elt in dictionary(node)['outs'].elts)
                stripped = tuple(out.removeprefix('gen/' + cpu + '/') for out in outputs)
                if any(a == b for a, b in zip(outputs, stripped, strict=True)):
                    raise ValueError(('Generator has outputs outside its CPU directory', name))
                if expected is not None and expected != stripped:
                    raise ValueError(('Output names or order differ', name))
                expected = stripped
            self.eligible[family] = pair
            for output in expected:
                if output in self.output_paths:
                    raise ValueError(('Two native generators produce', output))
                self.output_paths.add(output)
                self.output_directories.update(p.as_posix() for p in Path(output).parents)
            self.facts[family] = len(expected)
            for name, _ in pair.values():
                self.rename_rules[name] = 'generate_native_' + (
                    'clickhouse_grpc' if family == 'a11a7a9f459c326c' else family)
        if not self.eligible:
            return
        outputs = {cpu: text_constant(dictionary(record).get('out')) for cpu, record in self.options.items()}
        suffixes = {out.removeprefix('gen/' + cpu + '/') for cpu, out in outputs.items() if out}
        if len(suffixes) != 1 or not all(outputs.values()):
            raise ValueError(('Unexpected paired build-options outputs', outputs))
        output = suffixes.pop()
        self.options_output = 'gen/' + output
        self.output_paths.add(output)
        self.output_directories.update(p.as_posix() for p in Path(output).parents)
        # Never map directories containing an unpaired output: the x86 HDFS
        # assembler action still owns gen/x86_64/contrib/libhdfs3-cmake/*.o.
        for family, pair in families.items():
            if family in self.eligible:
                continue
            for cpu, (_, node) in pair.items():
                for elt in dictionary(node)['outs'].elts:
                    out = text_constant(elt)
                    if out.startswith('gen/' + cpu + '/'):
                        suffix = out.removeprefix('gen/' + cpu + '/')
                        self.output_directories.difference_update(p.as_posix() for p in Path(suffix).parents)
        all_outputs = {
            text_constant(elt)
            for node in raw.values() if isinstance(node, ast.Dict) and 'outs' in dictionary(node)
            for elt in dictionary(node)['outs'].elts
        }
        collisions = {'gen/' + out for out in self.output_paths} & all_outputs
        if collisions:
            raise ValueError(('Output collision', sorted(collisions)))

    def value(self, value, owner):
        # Next.js filenames include spaces and ')'; first try the complete
        # label/output. The word scan below handles directories in commands.
        literal = re.fullmatch(r'(.*?gen/)(x86_64|aarch64|\{arch\})/(.*)', value)
        if literal and '\n' not in value:
            suffix = literal[3].rstrip('/')
            if suffix in self.output_paths or suffix in self.output_directories:
                return literal[1] + literal[3]

        def generated(match):
            suffix = match[2].rstrip('/')
            if suffix in self.output_paths or suffix in self.output_directories:
                return 'gen/' + match[2]
            return match[0]
        result = re.sub(r'gen/(x86_64|aarch64|\{arch\})/([^\s"\x27,\x29\x5d]+)', generated, value)
        for prefix in ('@clickhouse//bazel:', '//bazel:', ':'):
            if (prefix != ':' or owner == 'bazel') and result.startswith(prefix):
                name = result[len(prefix):]
                if name in self.rename_rules:
                    return prefix + self.rename_rules[name]
        return result

    def action_key(self, attrs, cpu, cmake, old=False):
        resolver = ConfiguredAction(self, cpu, cmake)
        fields = {}
        for key, node in attrs.items():
            value = copy.deepcopy(node)
            if old:
                value = MapConstants(self, None).visit(value)
            fields[key] = resolver.visit(value)
        if old and 'cmake_cmd' in fields:
            fields['cmd'] = resolver.visit(ast.BinOp(
                left=fields['cmd'], op=ast.Add(),
                right=fields['cmake_cmd'] if cmake else ast.Constant('')))
        if old and text_constant(fields.get('kind')) == 'licenses' and not cmake:
            fields['cmake_metadata'] = ast.Constant(None)
        fields.pop('cmake_cmd', None)
        fields.pop('cmake_condition', None)
        cmd = fields.get('cmd')
        if isinstance(cmd, ast.BinOp) and isinstance(cmd.op, ast.Add) and text_constant(cmd.right) == '':
            fields['cmd'] = cmd.left
        return {key: ast_key(value) for key, value in fields.items()}

    def codegen(self, source):
        entries = []
        emitted = set()
        for name, node in self.original.items():
            shared = self.rename_rules.get(name)
            if not shared:
                if (isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'codegen_for_arch'):
                    raise ValueError(('Unpaired legacy template still needs codegen_for_arch', name))
                if isinstance(node, ast.Dict) and {
                    'cmake_cmd', 'cmake_condition', 'cmake_metadata',
                } & dictionary(node).keys():
                    raise ValueError(('Unpaired generator still needs metadata selection', name))
                entries.append(json.dumps(name) + ': ' + source.text(node) + ',')
                continue
            if shared in emitted:
                continue
            emitted.add(shared)
            family = next(key for key, pair in self.eligible.items()
                          if name in {value[0] for value in pair.values()})
            records = {cpu: dictionary(row[1]) for cpu, row in self.eligible[family].items()}
            values = {}
            for attr in records[CPUS[0]]:
                if attr in {'cmake_condition', 'cmake_cmd'}:
                    continue
                by_cpu = {cpu: MapConstants(self, None).visit(copy.deepcopy(fields[attr]))
                          for cpu, fields in records.items()}
                if attr == 'cmake_metadata' and family == 'licenses':
                    values[attr] = 'select({' + ''.join(
                        json.dumps('//bazel:use_cmake_metadata_' + cpu) + ': ' +
                        '"//bazel/initial_migration:" + CMAKE_METADATA[' + json.dumps(cpu) + '].get(' +
                        '"license_substitutions", "license_substitutions_' + cpu + '.json"),'
                        for cpu in CPUS
                    ) + '"//conditions:default": None,})'
                elif all(ast_key(value) == ast_key(by_cpu[CPUS[0]]) for value in by_cpu.values()):
                    values[attr] = ast.unparse(by_cpu[CPUS[0]])
                else:
                    values[attr] = select_cpu({cpu: ast.unparse(value) for cpu, value in by_cpu.items()})
                if attr == 'cmd' and 'cmake_cmd' in records[CPUS[0]]:
                    values[attr] = '(' + values[attr] + ') + select({' + ''.join(
                        json.dumps('//bazel:use_cmake_metadata_' + cpu) + ': ' +
                        '" --cmake-gzip-mtime=" + str(CMAKE_METADATA[' + json.dumps(cpu) + ']' +
                        '["resource_gzip_mtimes"][' + json.dumps(family) + ']),'
                        for cpu in CPUS
                    ) + '"//conditions:default": "",})'
            parsed = {key: ast.parse(value, mode='eval').body for key, value in values.items()}
            for cpu in CPUS:
                for cmake in (False, True):
                    one = self.action_key(records[cpu], cpu, cmake, old=True)
                    two = self.action_key(parsed, cpu, cmake)
                    if one != two:
                        raise ValueError(('Generated action changed', family, cpu, cmake,
                                          {key for key in one.keys() | two.keys() if one.get(key) != two.get(key)}))
                    self.counts['generator_actions_checked'] += 1
            entries.append(json.dumps(shared) + ': {' + ''.join(
                json.dumps(key) + ': ' + value + ',' for key, value in values.items()) + '},')
        edits = [(*source.span(self.definitions['CODEGEN_RULES']), '{' + ''.join(entries) + '}')]
        # ExternalCodegen already consumed contrib templates. Native grpc and
        # embedded templates were the last callers of codegen_for_arch.
        shared = self.definitions.get('_SHARED_CODEGEN')
        if shared is not None:
            statement = next(stmt for stmt in source.module.body
                             if isinstance(stmt, ast.Assign) and stmt.value is shared)
            edits.append((*source.span(statement), ''))
        for stmt in source.module.body:
            if (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call)
                    and getattr(stmt.value.func, 'id', '') == 'load'
                    and any(text_constant(arg) == 'codegen_for_arch' for arg in stmt.value.args)):
                edits.append((*source.span(stmt), ''))
        result = source.rewrite(edits)
        return 'load("@clickhouse//bazel/initial_migration:cmake_metadata.bzl", "CMAKE_METADATA")\n' + result

    def format_text(self, content, kind):
        return subprocess.run(
            ['buildifier', '-mode=fix', '-type=' + kind, '-config=' + str(self.root / '.buildifier.json')],
            input=content, text=True, check=True, stdout=subprocess.PIPE).stdout

    def format_data(self, content):
        """Start each recorded dictionary/list entry on a separate line."""
        source = Source(content)

        def render(node, indent):
            pad = ' ' * indent
            if isinstance(node, ast.List):
                if not node.elts:
                    return '[]'
                return '[\n' + ''.join(pad + '    ' + render(item, indent + 4) + ',\n'
                                        for item in node.elts) + pad + ']'
            if isinstance(node, ast.Dict):
                if not node.keys:
                    return '{}'
                return '{\n' + ''.join(
                    pad + '    ' + source.text(key) + ': ' + render(value, indent + 4) + ',\n'
                    for key, value in zip(node.keys, node.values, strict=True)
                ) + pad + '}'
            start, end = source.span(node)
            edits = []
            # Also format select dictionaries inside cmd concatenations. Only
            # outermost nested lists/dictionaries are replaced in each pass.
            for child in ast.walk(node):
                if child is node or not isinstance(child, (ast.List, ast.Dict)):
                    continue
                lower, upper = source.span(child)
                if any(first <= lower and upper <= last for first, last, _ in edits):
                    continue
                edits.append((lower, upper, render(child, indent)))
            position = start
            parts = []
            for lower, upper, value in sorted(edits):
                parts.extend((source.bytes[position:lower], value.encode()))
                position = upper
            parts.append(source.bytes[position:end])
            return b''.join(parts).decode()

        edits = [
            (*source.span(stmt.value), render(stmt.value, 0))
            for stmt in source.module.body if isinstance(stmt, ast.Assign)
        ]
        result = self.format_text(source.rewrite(edits), 'bzl')
        if ast_key(source.module) != ast_key(Source(result).module):
            raise ValueError('Formatting changed generated data')
        return result

    def starlark(self, path, source):
        owner = 'bazel' if path == self.root / 'bazel/BUILD.bazel' else None
        edits = []
        for node in ast.walk(source.module):
            old = text_constant(node)
            if old is None:
                continue
            new = self.value(old, owner)
            if new != old:
                edits.append((*source.span(node), json.dumps(new, ensure_ascii=False)))
        if not edits:
            return None
        translated = source.rewrite(edits)
        # Use the same exact-list proof as normal BUILD factoring, with srcs
        # admitted only for this operation. The generator itself is sequential.
        original_fields = selections.FIELDS
        try:
            selections.FIELDS = original_fields | {'srcs'}
            result = selections.join(selections.factor(translated, self.counts))
            result = self.format_text(result, 'build' if path.name.endswith('BUILD.bazel') else 'bzl')
            selections.prove(translated, result, path)
        finally:
            selections.FIELDS = original_fields
        self.counts['consumer_constants_mapped'] += len(edits)
        self.counts['consumer_files'] += 1
        return result

    def options_build(self, source):
        comps = [node for node in ast.walk(source.module) if isinstance(node, ast.ListComp)
                 and getattr(getattr(node.elt, 'func', None), 'id', '') == 'build_options']
        if not comps:
            # bazel/BUILD is checked in and not overwritten by generate_build.
            # Regeneration only replaces BUILD_OPTIONS and CODEGEN_RULES.
            existing = [call for _, call in source.rules()
                        if getattr(call.func, 'id', '') == 'build_options']
            if (len(existing) != 1 or text_constant(attributes(existing[0]).get('out')) != self.options_output
                    or text_constant(attributes(existing[0]).get('name')) != 'build_options'):
                raise ValueError('Missing configured //bazel:build_options')
            return source.bytes.decode()
        if len(comps) != 1:
            raise ValueError(('Unexpected build_options comprehensions', len(comps)))
        original = comps[0]
        fields = attributes(original.elt)
        values = {'name': json.dumps('build_options'), 'out': json.dumps(self.options_output)}
        for field in fields:
            if field in {'name', 'out', 'target_compatible_with'}:
                continue
            if field in {'template', 'c_flags', 'cxx_flags', 'link_flags'}:
                values[field] = select_cpu({cpu: 'BUILD_OPTIONS[' + json.dumps(cpu) + '][' +
                                           json.dumps(field) + ']' for cpu in CPUS})
            elif field == 'cmake_substitutions':
                values[field] = select_cpu({cpu: 'CMAKE_METADATA[' + json.dumps(cpu) +
                                            ']["build_options"]' for cpu in CPUS})
            else:
                values[field] = source.text(fields[field])
        expr = 'build_options(' + ''.join(key + '=' + value + ',' for key, value in values.items()) + ')'
        actual = attributes(ast.parse(expr, mode='eval').body)
        for cpu in CPUS:
            expected = {key: BuildOptionBindings(self, cpu).visit(copy.deepcopy(value))
                        for key, value in fields.items()}
            for cmake in (False, True):
                before = self.action_key(expected, cpu, cmake, old=True)
                after = self.action_key(actual, cpu, cmake)
                before.pop('name')
                after.pop('name')
                compat = before.pop('target_compatible_with')
                if compat != ast_key(ast.List(elts=[ast.Constant('@platforms//cpu:' + cpu)], ctx=ast.Load())):
                    raise ValueError(('Unexpected original CPU compatibility', compat))
                if before != after:
                    raise ValueError(('Changed build_options action', cpu, cmake,
                                      {key for key in before.keys() | after.keys()
                                       if before.get(key) != after.get(key)}))
                self.counts['build_options_actions_checked'] += 1
        result = source.rewrite([(*source.span(original), expr)])
        old = 'if name.startswith("generate_" + arch + "_") or name.startswith("generate_contrib_")'
        if result.count(old) != 1:
            raise ValueError('Cannot identify historical generated_<arch> filegroups')
        result = result.replace(old, old + ' or name.startswith("generate_native_")')
        old = 'srcs = [":build_options_" + arch]'
        if result.count(old) != 1:
            raise ValueError('Cannot identify build_options in generated_<arch> groups')
        result = result.replace(old, 'srcs = [":build_options"]')
        return result

    def bazel_build(self):
        path = self.root / 'bazel/BUILD.bazel'
        source = Source(self.options_build(Source(path.read_text())))
        web = [call for _, call in source.rules()
               if text_constant(attributes(call).get('name')) == 'web_ui_resources']
        if len(web) > 1:
            raise ValueError('Duplicate web_ui_resources')
        if not web:
            return path, self.format_text(source.bytes.decode(), 'build')
        from initial_migration.web_resources import GROUP_BUILD
        expected = next(call for _, call in Source(GROUP_BUILD).rules())
        old = web[0]
        if ast_key(old) == ast_key(expected):
            return path, self.format_text(source.bytes.decode(), 'build')
        for cpu in CPUS:
            resolver = ConfiguredAction(self, cpu, False)
            initial = resolver.visit(MapConstants(self, 'bazel').visit(copy.deepcopy(attributes(old)['srcs'])))
            final = resolver.visit(copy.deepcopy(attributes(expected)['srcs']))
            if ast_key(initial) != ast_key(final):
                raise ValueError(('Web resource group changed', cpu))
        result = source.rewrite([(*source.span(old), Source(GROUP_BUILD).text(expected))])
        return path, self.format_text(result, 'build')

    def parity_metadata(self):
        path = self.root / 'bazel/initial_migration/any2bazel.json'
        data = json.loads(path.read_text())
        identities = {
            '@build/' + cpu + '/' + output: '@build/' + output
            for cpu in CPUS for output in self.output_paths
        }
        result = {}
        for row in data['ignore']['source_map']:
            name, identity = row['from'], row['to']
            new_name, new_identity = self.value(name, None), identities.get(identity, identity)
            if new_name != name:
                self.counts['source_names_mapped'] += 1
            if new_identity != identity:
                self.counts['build_source_identities_mapped'] += 1
            elif new_name != name:
                self.counts['copied_source_identities_preserved'] += 1
            if new_name in result and result[new_name] != new_identity:
                raise ValueError(('Conflicting source identity', name, new_name, result[new_name], new_identity))
            result[new_name] = new_identity
        data['ignore']['source_map'] = [{'from': name, 'to': identity} for name, identity in result.items()]
        includes = {row['from']: row['to'] for row in data['ignore']['include_map']}
        for prefix in ('src', 'embedded'):
            name, identity = 'bazel-out/k8-opt/bin/bazel/gen/' + prefix, '@build/' + prefix
            if name in includes and includes[name] != identity:
                raise ValueError(('Conflicting generated include identity', name))
            includes[name] = identity
        data['ignore']['include_map'] = [{'from': name, 'to': identity} for name, identity in includes.items()]
        return path, json.dumps(data, indent=2) + '\n'

    def prepare(self):
        source = Source(self.path.read_text())
        self.collect(source)
        if not self.eligible:
            return {}
        changes = {self.path: self.format_data(self.codegen(source))}
        path, result = self.bazel_build()
        if result != path.read_text():
            changes[path] = result
        path = self.root / 'bazel/rules/build_options_data.bzl'
        data = Source(path.read_text())
        defs = data_assignments(data)
        opts = dictionary(defs['BUILD_OPTIONS'])
        values = {}
        for cpu, record in opts.items():
            values[cpu] = {key: data.text(value) for key, value in dictionary(record).items() if key != 'out'}
        rendered = '{' + ''.join(
            json.dumps(cpu) + ': {' + ''.join(json.dumps(key) + ': ' + val + ','
                                               for key, val in fields.items()) + '},'
            for cpu, fields in values.items()
        ) + '}'
        changes[path] = self.format_data(data.rewrite([(*data.span(defs['BUILD_OPTIONS']), rendered)]))
        files = subprocess.check_output(
            ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard', '--', '*.bazel', '*.bzl'],
            cwd=self.root).decode().split('\0')
        for filename in files:
            if not filename:
                continue
            path = self.root / filename
            if path in changes or path == self.root / 'bazel/BUILD.bazel' or not path.is_file():
                continue
            text = path.read_text()
            if not ('gen/x86_64' in text or 'gen/aarch64' in text):
                continue
            changed = self.starlark(path, Source(text))
            if changed is not None and changed != text:
                changes[path] = changed
        path, result = self.parity_metadata()
        changes[path] = result
        return changes

    def run(self, write=False, *, read_only=()):
        changes = self.prepare()
        if set(changes).intersection(read_only):
            raise ValueError('Update native codegen labels in the maintained BUILD file before importing')
        print(json.dumps({'written': write, 'paired_generators': self.facts, 'outputs': len(self.output_paths),
                          'proof': self.counts, 'files': [str(path.relative_to(self.root))
                                                         for path in sorted(changes)]}, indent=2))
        if write:
            for path, result in changes.items():
                path.write_text(result)
        return changes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    Migration(args.root).run(write=args.write)


if __name__ == '__main__':
    main()

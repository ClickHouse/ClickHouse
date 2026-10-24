#!/usr/bin/env python3
"""Give the library compiling WebUIRequestHandler.cpp a private dependency on its resources."""
import argparse
import ast
import copy
import json
import posixpath
from pathlib import Path
import sys

RULES = {'ch_cc_library', 'ch_cc_binary', 'ch_cc_test', 'cc_library', 'cc_binary', 'cc_test'}
CPUS = ('x86_64', 'aarch64')
GROUP = '//bazel:web_ui_resources'
ASSETS = '//bazel:web_ui_embedded_assets'
FIELDS = {'hdrs', 'textual_hdrs', 'additional_compiler_inputs', 'local_includes', 'includes'}
NAMESPACES = {'ClickStack': 'clickstack', 'SQLConsole': 'sql-console'}
GROUP_BUILD = """
filegroup(
    name = "clickstack_header",
    srcs = [":generate_native_ClickStack"],
    output_group = "header",
)

filegroup(
    name = "clickstack_assets",
    srcs = [":generate_native_ClickStack"],
    output_group = "assets",
)

filegroup(
    name = "web_ui_embedded_assets",
    srcs = [":clickstack_assets"] + [
        out
        for out in CODEGEN_RULES["generate_native_SQLConsole"]["outs"]
        if out.endswith(".gz")
    ],
    visibility = ["//src:__pkg__"],
)

cc_library(
    name = "web_ui_resources",
    hdrs = [
        ":clickstack_header",
        "gen/contrib/sql-console-cmake/SQLConsoleResources.generated.h",
    ],
    additional_compiler_inputs = [":web_ui_embedded_assets"],
    includes = [
        "gen/contrib/sql-console-cmake",
        "gen/contrib/clickstack-cmake",
    ],
    visibility = ["//src:__pkg__"],
)
"""
CLICKSTACK_LOAD = 'load("//bazel/rules/clickstack:embed.bzl", "embed_clickstack")'
CLICKSTACK_BUILD = """
embed_clickstack(
    name = "generate_native_ClickStack",
    src = "//bazel/rules/clickstack:source_export",
    gzip_mtime = select({
        ":use_cmake_metadata_x86_64": CMAKE_METADATA["x86_64"]["resource_gzip_mtimes"]["ClickStack"],
        ":use_cmake_metadata_aarch64": CMAKE_METADATA["aarch64"]["resource_gzip_mtimes"]["ClickStack"],
        "//conditions:default": -1,
    }),
)
"""


def define_paths(root):
    sys.path.insert(0, str(root / 'bazel'))
    from initial_migration.compact_build import Source
    return Source


def constant_defs(module):
    return {
        node.targets[0].id: node
        for node in module.body
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
    }


def rule_kind(call):
    return getattr(getattr(call, 'func', None), 'id', '')


def rule_attrs(call):
    return {kw.arg: kw.value for kw in call.keywords}


def constant_dict_field(node, constants):
    """Return the literal dict keyword selected by constant["field"], or None."""
    if not (isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant)
            and isinstance(node.slice.value, str)):
        return None
    value = constants.get(node.value.id) if isinstance(node.value, ast.Name) else node.value
    if rule_kind(value) != 'dict' or value.args:
        return None
    fields = rule_attrs(value)
    if None in fields or len(fields) != len(value.keywords):
        return None
    return fields.get(node.slice.value)


def rule_name(call):
    return getattr(rule_attrs(call).get('name'), 'value', '')


def string_constants(node):
    return [n.value for n in ast.walk(node) if isinstance(n, ast.Constant) and isinstance(n.value, str)]


def resource_file(value):
    if not value.startswith('//bazel:gen/'):
        return False
    return any(
        '/contrib/' + directory + '-cmake/' in value and
        (value.endswith('/' + ns + 'Resources.generated.h')
         or '/' + directory.replace('-', '_') + '_compressed/' in value
         or value.endswith('/' + directory.replace('-', '_') + '_compressed'))
        for ns, directory in NAMESPACES.items()
    )


def select_dict(node):
    if (isinstance(node, ast.Call) and rule_kind(node) == 'select' and len(node.args) == 1
            and not node.keywords and isinstance(node.args[0], ast.Dict)):
        return node.args[0]
    return None


class Configured(ast.NodeTransformer):
    """Evaluate constants and CPU selects but leave globs and other calls opaque."""

    def __init__(self, definitions, cpu):
        self.defs = definitions
        self.cpu = cpu
        self.stack = []

    def visit_Name(self, node):
        if node.id not in self.defs:
            return node
        if node.id in self.stack:
            raise ValueError('constant cycle: ' + node.id)
        self.stack.append(node.id)
        result = self.visit(copy.deepcopy(self.defs[node.id].value))
        self.stack.pop()
        return result

    def visit_Call(self, node):
        dictionary = select_dict(node)
        if dictionary:
            pairs = {getattr(k, 'value', None): v for k, v in zip(dictionary.keys, dictionary.values, strict=True)}
            if all(k in {'@platforms//cpu:x86_64', '@platforms//cpu:aarch64', '//conditions:default'} for k in pairs):
                value = pairs.get('@platforms//cpu:' + self.cpu, pairs.get('//conditions:default'))
                if value is not None:
                    return self.visit(copy.deepcopy(value))
        return self.generic_visit(node)

    def visit_BinOp(self, node):
        node = self.generic_visit(node)
        if isinstance(node.op, ast.Add):
            if isinstance(node.left, ast.List) and not node.left.elts:
                return node.right
            if isinstance(node.right, ast.List) and not node.right.elts:
                return node.left
            if isinstance(node.left, ast.List) and isinstance(node.right, ast.List):
                return ast.List(elts=node.left.elts + node.right.elts, ctx=ast.Load())
        return node

    def visit_Subscript(self, node):
        node = self.generic_visit(node)
        field = constant_dict_field(node, {})
        return node if field is None else field


def key(node):
    return ast.dump(node, include_attributes=False)


def eval_node(node, definitions, cpu):
    return Configured(definitions, cpu).visit(copy.deepcopy(node or ast.List(elts=[], ctx=ast.Load())))


def expanded_inputs(node, definitions, cpu):
    return string_constants(eval_node(node, definitions, cpu))


def codegen_table(root, Source):
    source = Source((root / 'bazel/rules/codegen_rules.bzl').read_text())
    tables = [value for node in source.module.body if isinstance(node, ast.Assign)
              and any(isinstance(t, ast.Name) and t.id == 'CODEGEN_RULES' for t in node.targets)
              for value in [node.value] if isinstance(value, ast.Dict)]
    if len(tables) != 1:
        raise ValueError('expected one literal CODEGEN_RULES table')
    return source, tables[0]


def generator_outputs(root, Source):
    _, entries = codegen_table(root, Source)
    table = {key.value: value for key, value in zip(entries.keys, entries.values, strict=True)
             if isinstance(key, ast.Constant)}
    result = {}
    for cpu in CPUS:
        outputs = set()
        for ns in NAMESPACES:
            gen = table.get('generate_native_' + ns)
            if gen is None and ns == 'ClickStack':
                labels = {
                    '//bazel:gen/contrib/clickstack-cmake/ClickStackResources.generated.h',
                    '//bazel:gen/contrib/clickstack-cmake/clickstack_compressed',
                }
            else:
                attrs = {k.value: v for k, v in zip(gen.keys, gen.values, strict=True)}
                labels = {'//bazel:' + path for path in ast.literal_eval(attrs['outs'])}
            if not labels or not all(resource_file(label) for label in labels):
                raise ValueError('unexpected generator outputs: ' + ns + ' ' + cpu)
            if outputs & labels:
                raise ValueError('duplicate Web UI generator outputs')
            outputs |= labels
        result[cpu] = outputs
    return result


def publish_clickstack(root, Source, bazel_text):
    """Replace the recorded genrule after its outputs passed the owner proof."""
    changes = {}
    source, table = codegen_table(root, Source)
    edits = []
    for name, value in zip(table.keys, table.values, strict=True):
        if isinstance(name, ast.Constant) and name.value == 'generate_native_ClickStack':
            start = source.span(name)[0]
            end = source.span(value)[1]
            if source.bytes[end:end + 1] != b',':
                raise ValueError('expected a comma after generate_native_ClickStack')
            edits.append((start, end + 1, ''))
    if edits:
        changes[root / 'bazel/rules/codegen_rules.bzl'] = source.rewrite(edits)

    # The pinned export is no longer a build input. Its repository still
    # supplies LICENSE to generate_licenses.
    exported = root / 'bazel/dependencies/clickhouse_clickstack.BUILD.bazel'
    export_source = Source(exported.read_text())
    old_names = []
    for statement in export_source.module.body:
        if not isinstance(statement, ast.Expr):
            raise ValueError('unexpected ClickStack repository declaration')
        name = rule_kind(statement.value)
        if name == 'exports_files':
            old_names.extend(ast.literal_eval(statement.value.args[0]))
        elif name != 'package':
            raise ValueError('unexpected ClickStack repository rule: ' + name)
    if old_names.count('LICENSE') != 1 or any(name != 'LICENSE' and not name.startswith('out/')
                                               for name in old_names):
        raise ValueError('unexpected ClickStack repository exports')
    changes[exported] = ('package(default_visibility = ["//visibility:public"])\n\n'
                         'exports_files(["LICENSE"])\n')

    source = Source(bazel_text)
    edits = []
    loads = [s for s in source.module.body if isinstance(s, ast.Expr) and rule_kind(s.value) == 'load']
    if CLICKSTACK_LOAD not in bazel_text:
        if not loads:
            raise ValueError('missing bazel/BUILD.bazel loads')
        end = source.span(loads[-1])[1]
        edits.append((end, end, '\n' + CLICKSTACK_LOAD))
    existing = [rule for _, rule in source.rules() if rule_name(rule) == 'generate_native_ClickStack']
    if not existing:
        end = len(source.bytes)
        edits.append((end, end, '\n' + CLICKSTACK_BUILD))
    elif len(existing) != 1 or key(existing[0]) != key(next(Source(CLICKSTACK_BUILD).rules())[1]):
        raise ValueError('unexpected ClickStack generator declaration')
    groups = []
    for node in ast.walk(source.module):
        if rule_kind(node) != 'filegroup':
            continue
        attrs = rule_attrs(node)
        if string_constants(attrs.get('name', ast.Constant(None))) == ['generated_']:
            groups.append(attrs['srcs'])
    if len(groups) != 1:
        raise ValueError('expected one generated_<arch> filegroup comprehension')
    srcs = groups[0]
    if ':generate_native_ClickStack' not in string_constants(srcs):
        start, end = source.span(srcs)
        edits.append((start, end, '[' + repr(':generate_native_ClickStack') + '] + (' + source.text(srcs) + ')'))
    changes[root / 'bazel/BUILD.bazel'] = source.rewrite(edits)
    return changes


def literal_string(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = literal_string(node.left), literal_string(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def is_migrated_input(package, item, field):
    if field in {'local_includes', 'includes'}:
        # Native code includes generated headers through their C++ owners.
        value = literal_string(item)
        if value is None:
            return False
        path = posixpath.normpath(posixpath.join(package, value))
        return path in {
            'bazel/gen', 'bazel/gen/x86_64', 'bazel/gen/aarch64',
            'bazel/gen/contrib/clickstack-cmake', 'bazel/gen/contrib/sql-console-cmake',
        }
    return isinstance(item, ast.Constant) and isinstance(item.value, str) and (
        resource_file(item.value) or item.value in {GROUP, '//bazel:clickstack_header'}
    )


def rewrite(text, rel, Source, owners):
    source = Source(text)
    defs = constant_defs(source.module)
    package = posixpath.dirname(rel)
    edits = {}
    visited = set()

    def remove_inputs(node, field):
        if node is None or (id(node), field) in visited:
            return
        visited.add((id(node), field))
        if isinstance(node, ast.Name) and node.id in defs:
            remove_inputs(defs[node.id].value, field)
        elif isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name) and node.value.id in defs:
            table = defs[node.value.id].value
            if rule_kind(table) == 'dict' and isinstance(node.slice, ast.Constant):
                remove_inputs(rule_attrs(table).get(node.slice.value), field)
        elif isinstance(node, ast.List):
            keep = [item for item in node.elts if not is_migrated_input(package, item, field)]
            if len(keep) != len(node.elts):
                edits[source.span(node)] = '[' + ', '.join(source.text(item) for item in keep) + ']'
        else:
            for child in ast.iter_child_nodes(node):
                remove_inputs(child, field)

    for _, rule in source.rules():
        if rule_kind(rule) not in RULES:
            continue
        attrs = rule_attrs(rule)
        for field in FIELDS | {'srcs'}:
            remove_inputs(attrs.get(field), field)
        consumed = [any(s.endswith('/Server/WebUIRequestHandler.cpp')
                        for s in expanded_inputs(attrs.get('srcs'), defs, cpu)) for cpu in CPUS]
        if any(consumed):
            if not all(consumed):
                raise ValueError(f'{rel}: WebUIRequestHandler.cpp must have one owner on both CPUs')
            owners.append((rel, rule_name(rule)))

    if not edits and not any(path == rel for path, _ in owners):
        return text
    source = Source(source.rewrite([(a, b, v) for (a, b), v in edits.items()]))
    defs = constant_defs(source.module)
    edits = []
    for _, rule in source.rules():
        if (rel, rule_name(rule)) not in owners:
            continue
        attrs = rule_attrs(rule)
        missing = []
        # cc_library propagates the headers and includes, but only the consumer's
        # additional_compiler_inputs reach the action running Clang's #embed.
        for field, label in [('implementation_deps', GROUP), ('additional_compiler_inputs', ASSETS)]:
            value = attrs.get(field)
            if value is None:
                missing.append(f'{field} = ["{label}"],')
                continue
            counts = [expanded_inputs(value, defs, cpu).count(label) for cpu in CPUS]
            if counts == [1, 1]:
                continue
            if any(counts):
                raise ValueError(f'{rel}: expected one {label} on each CPU, got {counts}')
            if isinstance(value, ast.List):
                end = source.span(value.elts[-1])[1] if value.elts else source.span(value)[0] + 1
                edits.append((end, end, (', ' if value.elts else '') + f'"{label}"'))
            else:
                edits.append((*source.span(value), source.text(value) + f' + ["{label}"]'))
        if missing:
            end = source.span(rule)[1] - 1
            edits.append((end, end, ' '.join(missing)))
    for call in ast.walk(source.module):
        if not isinstance(call, ast.Call):
            continue
        for kw in call.keywords:
            if kw.arg != 'local_includes':
                continue
            for cpu in CPUS:
                value = eval_node(kw.value, defs, cpu)
                if not isinstance(value, ast.List) or value.elts:
                    break
            else:
                start, end = source.span(kw)
                edits.append((start, end + (source.bytes[end:end + 1] == b','), ''))
    return source.rewrite(edits)


def prepare(root, paths=None):
    Source = define_paths(root)
    generator_outputs(root, Source)
    changes = {}
    owners = []
    for path in (paths if paths is not None else
                 (p for directory in ('base', 'programs', 'src')
                  for p in (root / directory).rglob('BUILD.bazel'))):
        before = path.read_text()
        if not any(marker in before for marker in ('bazel/gen', 'WebUIRequestHandler.cpp',
                                                   'clickstack_header', 'web_ui_resources')):
            continue
        after = rewrite(before, path.relative_to(root).as_posix(), Source, owners)
        if after != before:
            changes[path] = after
    if len(owners) != 1:
        raise ValueError('Expected one library compiling WebUIRequestHandler.cpp, got ' + repr(owners))

    bazel = root / 'bazel/BUILD.bazel'
    source = Source(bazel.read_text())
    rules = {rule_name(rule): rule for _, rule in source.rules()}
    edits = []
    append = []
    expected = {rule_name(rule): rule for _, rule in Source(GROUP_BUILD).rules()}
    for name, rule in expected.items():
        existing = rules.get(name)
        if existing is None:
            declaration = Source(GROUP_BUILD).text(rule)
            if name == 'web_ui_embedded_assets' and 'web_ui_resources' in rules:
                start = source.span(rules['web_ui_resources'])[0]
                edits.append((start, start, declaration + '\n\n'))
            else:
                append.append(declaration)
        elif (rule_kind(existing), {k: key(v) for k, v in rule_attrs(existing).items()}) != (
                rule_kind(rule), {k: key(v) for k, v in rule_attrs(rule).items()}):
            if name != 'web_ui_resources':
                raise ValueError('Unexpected rule for ' + name)
            if rule_kind(existing) == 'cc_library':
                # Migrate the former library that kept the payloads only on itself.
                old = rule_attrs(rule) | {
                    'additional_compiler_inputs': rule_attrs(expected['web_ui_embedded_assets'])['srcs'],
                }
                if {k: key(v) for k, v in rule_attrs(existing).items()} != {k: key(v) for k, v in old.items()}:
                    raise ValueError('Unexpected attributes on ' + name)
            elif rule_kind(existing) != 'filegroup':
                raise ValueError('Unexpected rule for ' + name)
            edits.append((*source.span(existing), Source(GROUP_BUILD).text(rule)))
    after = source.rewrite(edits)
    if append:
        after = after.rstrip() + '\n\n' + '\n\n'.join(append) + '\n'
    changes.update(publish_clickstack(root, Source, after))
    return {path: text for path, text in changes.items() if path.read_text() != text}, {
        'web_ui_consumer': owners[0], 'changed_builds': len(changes),
    }


def run(root, *, write=False, paths=None):
    changes, counts = prepare(root, paths)
    if write:
        for path, content in changes.items():
            path.write_text(content)
    return changes, counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--preview', type=Path, help='Mirror changed BUILD files here, leave --root untouched')
    options = parser.parse_args()
    changes, counts = run(options.root, write=options.write)
    if options.preview:
        for path, content in changes.items():
            target = options.preview / path.relative_to(options.root)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
    print(json.dumps({'stats': counts, 'files': [str(p.relative_to(options.root)) for p in changes]}, indent=2))


if __name__ == '__main__':
    main()

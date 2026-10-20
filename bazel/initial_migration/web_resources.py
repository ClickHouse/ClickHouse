#!/usr/bin/env python3
"""Keep Web UI compiler inputs only in the library compiling WebUIRequestHandler.cpp.

This migration runs after native BUILD rendering and pruning. It replaces the
captured ClickStack inventory with the source export after checking ownership.
"""
import argparse
import ast
import copy
import json
from collections import Counter, defaultdict
from pathlib import Path
import sys

RULES = {'ch_cc_library', 'cc_library', 'cc_binary', 'cc_test'}
CPUS = ('x86_64', 'aarch64')
CONSUMER = ('src/BUILD.bazel', 'dbms__compile_0')
GROUP = '//bazel:web_ui_resources'
FIELDS = {'hdrs', 'textual_hdrs', 'additional_compiler_inputs', 'local_includes', 'includes'}
FILE_FIELDS = FIELDS - {'local_includes', 'includes'}
NAMESPACES = {'ClickStack': 'clickstack', 'SQLConsole': 'sql-console'}
GROUP_BUILD = """
filegroup(
    name = "web_ui_resources",
    srcs = [
        ":generate_native_ClickStack",
        ":generate_native_SQLConsole",
    ] + select({
        "@platforms//cpu:x86_64": [],
        "@platforms//cpu:aarch64": [],
    }),
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


def resource_include(value):
    return 'bazel/gen/' in value and any(
        value.endswith('/contrib/' + directory + '-cmake') for directory in NAMESPACES.values()
    )


def removable(node, field):
    predicate = resource_file if field in FILE_FIELDS else resource_include
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return predicate(node.value)
    # A local_includes element can be a repository-relative helper or a
    # prefix followed by the include directory. Never interpret select().
    if (isinstance(node, (ast.Call, ast.BinOp)) and rule_kind(node) != 'select'
            and field not in FILE_FIELDS):
        return any(predicate(s) for s in string_constants(node))
    return False


def contains(node, field):
    return any(isinstance(n, ast.List) and any(removable(item, field) for item in n.elts) for n in ast.walk(node))


def select_dict(node):
    if (isinstance(node, ast.Call) and rule_kind(node) == 'select' and len(node.args) == 1
            and not node.keywords and isinstance(node.args[0], ast.Dict)):
        return node.args[0]
    return None


def join(*parts):
    parts = [part for part in parts if part != '[]']
    return ' + '.join('(' + part + ')' for part in parts) if parts else '[]'


def list_text(source, elements):
    if not elements:
        return '[]'
    return '[\n' + ''.join('    ' + source.text(element) + ',\n' for element in elements) + ']'


def render_select(source, dictionary, values):
    if all(value == '[]' for value in values):
        return '[]'
    return 'select({\n' + ''.join('    ' + source.text(k) + ': ' + v + ',\n'
                                   for k, v in zip(dictionary.keys, values, strict=True)) + '})'


def split_includes(source, node):
    """Split a shared expression, keeping all select conditions and ordering.

    The consumer inserts the generated include directories between the same
    prefix and suffix that all other libraries use. Large unrelated include
    lists occur once, even when the original constant was shared.
    """
    if not contains(node, 'local_includes'):
        return source.text(node), '[]', '[]'
    if isinstance(node, ast.List):
        indices = [i for i, item in enumerate(node.elts) if removable(item, 'local_includes')]
        if indices != list(range(indices[0], indices[-1] + 1)):
            raise ValueError('non-adjacent Web UI directories in a shared include expression')
        a, b = indices[0], indices[-1] + 1
        return tuple(list_text(source, x) for x in (node.elts[:a], node.elts[a:b], node.elts[b:]))
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = contains(node.left, 'local_includes'), contains(node.right, 'local_includes')
        if left and right:
            raise ValueError('Web UI directories in both operands of a shared include expression')
        if left:
            a, b, c = split_includes(source, node.left)
            return a, b, join(c, source.text(node.right))
        a, b, c = split_includes(source, node.right)
        return join(source.text(node.left), a), b, c
    dictionary = select_dict(node)
    if dictionary:
        choices = [split_includes(source, v) for v in dictionary.values]
        return tuple(render_select(source, dictionary, part) for part in zip(*choices))
    raise ValueError('unsupported shared Web UI include expression: ' + source.text(node))


def prune_lists(source, node, field, edits, counts):
    dictionary = select_dict(node)
    if (dictionary and dictionary.values
            and all(isinstance(value, ast.List) for value in dictionary.values)):
        items = [item for value in dictionary.values for item in value.elts]
        if items and all(removable(item, field) for item in items):
            counts[field] += len(items)
            edits.append((*source.span(node), '[]'))
            return
    if isinstance(node, ast.List):
        keep = [item for item in node.elts if not removable(item, field)]
        removed = len(node.elts) - len(keep)
        if removed:
            counts[field] += removed
            edits.append((*source.span(node), list_text(source, keep)))
            return
    for child in ast.iter_child_nodes(node):
        prune_lists(source, child, field, edits, counts)


class Configured(ast.NodeTransformer):
    """Evaluate constants and CPU selects but leave globs and other calls opaque."""

    def __init__(self, definitions, cpu, allowed_group=None):
        self.defs = definitions
        self.cpu = cpu
        self.allowed_group = allowed_group
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

    def visit_List(self, node):
        node = self.generic_visit(node)
        if self.allowed_group is not None:
            elts = []
            for item in node.elts:
                if isinstance(item, ast.Constant) and item.value == GROUP:
                    elts.extend(ast.Constant(value) for value in sorted(self.allowed_group))
                else:
                    elts.append(item)
            node.elts = elts
        return node


def key(node):
    return ast.dump(node, include_attributes=False)


def eval_node(node, definitions, cpu, group=None):
    return Configured(definitions, cpu, group).visit(copy.deepcopy(node or ast.List(elts=[], ctx=ast.Load())))


class Filter(ast.NodeTransformer):
    def __init__(self, field):
        self.field = field

    def visit_List(self, node):
        node.elts = [self.visit(item) for item in node.elts if not removable(item, self.field)]
        return node


def expanded_inputs(node, definitions, cpu, group=None):
    return string_constants(eval_node(node, definitions, cpu, group))


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


def prove(old, new, rel, generated, counts):
    originals = list(old.rules())
    rewritten = list(new.rules())
    if len(originals) != len(rewritten):
        raise ValueError(rel + ': changed BUILD rules')
    old_defs, new_defs = constant_defs(old.module), constant_defs(new.module)
    for (_, before), (_, after) in zip(originals, rewritten, strict=True):
        if (rule_name(before), rule_kind(before)) != (rule_name(after), rule_kind(after)):
            raise ValueError(rel + ': changed target name or kind')
        one, two = rule_attrs(before), rule_attrs(after)
        if one.keys() != two.keys():
            raise ValueError(rel + ': changed attributes of ' + rule_name(before))
        owner = (rel, rule_name(before)) == CONSUMER
        for cpu in CPUS:
            if rule_kind(before) in RULES:
                web_source = [s for s in expanded_inputs(one.get('srcs'), old_defs, cpu)
                              if s.endswith('/Server/WebUIRequestHandler.cpp')]
                if bool(web_source) != owner:
                    raise ValueError(f'{rel}:{rule_name(before)} {cpu} unexpected WebUIRequestHandler.cpp ownership')
            for field in one:
                initial = eval_node(one[field], old_defs, cpu)
                final = eval_node(two[field], new_defs, cpu)
                if field in FILE_FIELDS and rule_kind(before) in RULES:
                    # The owner moves its resource files from public hdrs to
                    # the single private compiler input group.
                    expect = Filter(field).visit(copy.deepcopy(initial))
                    found = Filter(field).visit(copy.deepcopy(final))
                    if owner and field == 'additional_compiler_inputs':
                        for private in (expect, found):
                            private.elts = [el for el in private.elts
                                            if not (isinstance(el, ast.Constant) and el.value == GROUP)]
                    if key(expect) != key(found):
                        raise ValueError(f'{rel}:{rule_name(before)} {cpu} changed non-resource {field}')
                elif field in {'local_includes', 'includes'} and rule_kind(before) in RULES and not owner:
                    expect = Filter(field).visit(initial)
                    if key(expect) != key(final):
                        raise ValueError(f'{rel}:{rule_name(before)} {cpu} changed non-Web UI {field}')
                elif key(initial) != key(final):
                    raise ValueError(f'{rel}:{rule_name(before)} {cpu} changed protected {field}')
            old_files = set()
            new_files = set()
            for field in FILE_FIELDS:
                old_files.update(filter(resource_file, expanded_inputs(one.get(field), old_defs, cpu)))
                new_files.update(filter(resource_file, expanded_inputs(two.get(field), new_defs, cpu, generated[cpu])))
            if owner:
                already = GROUP in expanded_inputs(one.get('additional_compiler_inputs'), old_defs, cpu)
                if already:
                    old_files.update(generated[cpu])
                if old_files != generated[cpu] or new_files != generated[cpu]:
                    raise ValueError(f'{rel}:{rule_name(before)} {cpu}: expected exact gzip/header generator outputs '
                                     f'oldmissing={sorted(generated[cpu] - old_files)} '
                                     f'oldextra={sorted(old_files - generated[cpu])} '
                                     f'newmissing={sorted(generated[cpu] - new_files)}')
                counts['owner_' + cpu + '_matched_files'] = len(new_files)
            elif new_files:
                raise ValueError(f'{rel}:{rule_name(before)} {cpu}: retained Web UI compiler files')
            counts['configured_rules'] += 1


def rewrite(text, rel, generated, Source, counts):
    source = Source(text)
    definitions = constant_defs(source.module)
    uses = defaultdict(list)
    affected = []
    for statement, rule in source.rules():
        if rule_kind(rule) not in RULES:
            continue
        owner = (rel, rule_name(rule)) == CONSUMER
        for kw in rule.keywords:
            for ref in ast.walk(kw.value):
                if isinstance(ref, ast.Name) and ref.id in definitions:
                    uses[ref.id].append((kw.arg, owner, ref))
            if kw.arg in FIELDS:
                affected.append((kw, owner, rule))

    edits = []
    for name, assignments in uses.items():
        original = definitions[name]
        fields = {field for field, _, _ in assignments}
        include = bool(fields & {'includes', 'local_includes'})
        files = bool(fields & FILE_FIELDS)
        if include and contains(original.value, 'local_includes'):
            if not fields <= {'local_includes', 'includes'}:
                raise ValueError(rel + ': mixed usage of include constant ' + name)
            owners = [ref for _, owner, ref in assignments if owner]
            if owners and any(not owner for _, owner, _ in assignments):
                prefix, dirs, suffix = split_includes(source, original.value)
                prefix_name = name + '_BEFORE_WEB'
                suffix_name = name + '_AFTER_WEB'
                if prefix_name in definitions or suffix_name in definitions:
                    raise ValueError(rel + ': shared include name collision')
                replacement = (prefix_name + ' = ' + prefix + '\n\n' +
                               suffix_name + ' = ' + suffix + '\n\n' +
                               name + ' = ' + join(prefix_name, suffix_name))
                edits.append((*source.span(original), replacement))
                for ref in owners:
                    edits.append((*source.span(ref), '(' + join(prefix_name, dirs, suffix_name) + ')'))
                counts['split_include_constants'] += 1
            elif not owners:
                prune_lists(source, original.value, 'local_includes', edits, counts)
        if files and contains(original.value, 'hdrs'):
            if not fields <= FILE_FIELDS:
                raise ValueError(rel + ': mixed usage of input constant ' + name)
            prune_lists(source, original.value, 'hdrs', edits, counts)

    for kw, owner, rule in affected:
        if kw.arg in FILE_FIELDS:
            prune_lists(source, kw.value, kw.arg, edits, counts)
        elif not owner:
            prune_lists(source, kw.value, kw.arg, edits, counts)
    result = source.rewrite(edits)
    source = Source(result)
    edits = []
    for _, rule in source.rules():
        if (rel, rule_name(rule)) != CONSUMER:
            continue
        attr = rule_attrs(rule)['additional_compiler_inputs']
        values = string_constants(attr)
        if GROUP not in values:
            edits.append((*source.span(attr), '(' + source.text(attr) + ') + ["' + GROUP + '"]'))
    result = source.rewrite(edits)
    prove(Source(text), Source(result), rel, generated, counts)
    return result


def prepare(root, paths=None):
    Source = define_paths(root)
    generated = generator_outputs(root, Source)
    counts = Counter()
    changes = {}
    filenames = list(paths) if paths is not None else [
        *root.glob('programs/**/BUILD.bazel'), *root.glob('src/**/BUILD.bazel'),
    ]
    for path in filenames:
        rel = path.relative_to(root).as_posix()
        before = path.read_text()
        if not any(ns + 'Resources.generated.h' in before or name.replace('-', '_') + '_compressed/' in before
                   or 'bazel/gen/' in before and '/contrib/' + name + '-cmake' in before
                   for ns, name in NAMESPACES.items()):
            continue
        after = rewrite(before, rel, generated, Source, counts)
        if after != before:
            changes[path] = after
            counts['build_files'] += 1
    bazel = root / 'bazel/BUILD.bazel'
    before = bazel.read_text()
    known = [rule for _, rule in Source(before).rules() if rule_name(rule) == 'web_ui_resources']
    if len(known) > 1:
        raise ValueError('duplicate web_ui_resources target')
    if not known:
        changes[bazel] = before.rstrip() + '\n' + GROUP_BUILD
    else:
        expected = list(Source(GROUP_BUILD).rules())[0][1]
        if key(known[0]) != key(expected):
            raise ValueError('unexpected web_ui_resources target')
    published = publish_clickstack(root, Source, changes.get(bazel, before))
    for path, text in published.items():
        if text != path.read_text():
            changes[path] = text
    return changes, counts


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

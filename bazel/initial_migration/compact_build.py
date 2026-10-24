#!/usr/bin/env python3
"""Compact generated BUILD attributes and verify their expanded expressions.

Run without --write to check and report the generated root and contrib BUILD
files.  --write replaces a file only after verifying every original statement.
BuildFile.text() uses the same transformation for future CMake regeneration.
"""

import argparse
import ast
from collections import defaultdict
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ATTRIBUTES = {
    "srcs", "hdrs", "textual_hdrs", "additional_compiler_inputs", "deps", "implementation_deps",
    "copts", "local_defines", "defines", "features", "linkopts",
    "target_compatible_with", "tools", "visibility", "includes", "local_includes", "data",
}
# These functions only calculate flags or repository roots.  In particular,
# never hoist glob(), Label(), a macro invocation, or an unknown identifier.
PURE_FUNCTIONS = {
    "select", "repository_path", "llvm_toolchain_path", "cmake_prefix_maps",
    "repository_local_include",
}
CONSTANT_PREFIX = "_GENERATED_"


def ast_key(node):
    return ast.dump(node, annotate_fields=True, include_attributes=False)


def safe_expression(node):
    if isinstance(node, ast.Constant):
        return isinstance(node.value, (str, int, bool)) or node.value is None
    if isinstance(node, ast.Name):
        return node.id == "CLICKHOUSE_WARNINGS"
    if isinstance(node, (ast.List, ast.Tuple)):
        return all(safe_expression(value) for value in node.elts)
    if isinstance(node, ast.Dict):
        return all(key is not None and safe_expression(key) and safe_expression(value)
                   for key, value in zip(node.keys, node.values))
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return safe_expression(node.left) and safe_expression(node.right)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        return (node.func.id in PURE_FUNCTIONS
                and all(safe_expression(arg) for arg in node.args)
                and all(kw.arg is not None and safe_expression(kw.value) for kw in node.keywords))
    return False


def select_dictionary(node):
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id == "select" and len(node.args) == 1 and not node.keywords
            and isinstance(node.args[0], ast.Dict) and node.args[0].values
            and all(isinstance(value, ast.List) for value in node.args[0].values)):
        return node.args[0]
    return None


def shared_cpu_selection(node):
    choices = select_dictionary(node)
    if choices is None:
        return None
    if not all(isinstance(key, ast.Constant) for key in choices.keys):
        return None
    cpus = {"@platforms//cpu:x86_64", "@platforms//cpu:aarch64"}
    keys = {key.value for key in choices.keys}
    if not (cpus <= keys <= cpus | {"//conditions:default"}):
        return None
    first = choices.values[0]
    if all(ast_key(value) == ast_key(first) for value in choices.values):
        return first
    return None


class Source:
    def __init__(self, text):
        # Python AST positions count UTF-8 bytes, including for non-ASCII names.
        self.bytes = text.encode("utf-8")
        self.starts = [0]
        for line in self.bytes.splitlines(keepends=True):
            self.starts.append(self.starts[-1] + len(line))
        self.module = ast.parse(text)

    def span(self, node):
        return (self.starts[node.lineno - 1] + node.col_offset,
                self.starts[node.end_lineno - 1] + node.end_col_offset)

    def text(self, node):
        start, end = self.span(node)
        return self.bytes[start:end].decode("utf-8")

    def rules(self):
        for statement in self.module.body:
            if (isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call)
                    and any(kw.arg == "name" for kw in statement.value.keywords)):
                yield statement, statement.value

    def rewrite(self, edits):
        chunks = []
        position = 0
        for start, end, replacement in sorted(edits):
            if start < position:
                raise ValueError("Overlapping edits in generated BUILD")
            chunks.extend((self.bytes[position:start], replacement.encode("utf-8")))
            position = end
        chunks.append(self.bytes[position:])
        return b"".join(chunks).decode("utf-8")


def render_list(source, values, indent):
    if not values:
        return "[]"
    pad = " " * indent
    return "[\n" + "".join(pad + "    " + source.text(value) + ",\n"
                            for value in values) + pad + "]"


def factor_selects(source):
    edits = []
    for _, rule in source.rules():
        for attr in rule.keywords:
            if attr.arg not in ATTRIBUTES:
                continue
            for node in ast.walk(attr.value):
                choices = select_dictionary(node)
                if choices is None or not safe_expression(node):
                    continue
                lists = [value.elts for value in choices.values]
                # Do not create nested edits. A list member containing select()
                # must stay within its own original select() expression.
                if any(select_dictionary(part) is not None
                       for values in lists for value in values for part in ast.walk(value)):
                    continue
                keys = [[ast_key(value) for value in values] for values in lists]
                limit = min(map(len, keys))
                prefix = 0
                while prefix < limit and all(items[prefix] == keys[0][prefix] for items in keys):
                    prefix += 1
                suffix = 0
                while (suffix < limit - prefix
                       and all(items[-suffix - 1] == keys[0][-suffix - 1] for items in keys)):
                    suffix += 1
                if not prefix and not suffix:
                    continue
                indent = attr.col_offset
                parts = []
                if prefix:
                    parts.append(render_list(source, lists[0][:prefix], indent))
                rows = []
                for key, values in zip(choices.keys, lists):
                    middle = values[prefix:len(values) - suffix]
                    rows.append(" " * (indent + 4) + source.text(key) + ": "
                                + render_list(source, middle, indent + 4) + ",\n")
                # Keep all condition labels, even if every branch becomes []:
                # select() must still reject a configuration that matched none.
                parts.append("select({\n" + "".join(rows) + " " * indent + "})")
                if suffix:
                    parts.append(render_list(source, lists[0][-suffix:], indent))
                # Preserve the original select's grouping after another
                # expression. The preceding select may contain glob() and
                # must not acquire this select's factored prefix or suffix.
                edits.append((*source.span(node), "(" + " + ".join(parts) + ")"))
    return source.rewrite(edits)


def intern_attributes(source):
    occurrences = defaultdict(list)
    used_names = {node.id for node in ast.walk(source.module) if isinstance(node, ast.Name)}
    for statement, rule in source.rules():
        for attr in rule.keywords:
            value = attr.value
            if (attr.arg in ATTRIBUTES and not isinstance(value, ast.Constant)
                    and safe_expression(value)):
                occurrences[(attr.arg, ast_key(value))].append((statement, value))
    edits = []
    declarations = defaultdict(list)
    numbers = defaultdict(int)
    for (attr, _), occurrences_for_attr in occurrences.items():
        if len(occurrences_for_attr) < 2:
            continue
        number = numbers[attr]
        while (name := f"{CONSTANT_PREFIX}{attr.upper()}_{number}") in used_names:
            number += 1
        statement, value = occurrences_for_attr[0]
        definition = name + " = " + source.text(value) + "\n\n"
        old_bytes = sum(end - start for _, item in occurrences_for_attr
                        for start, end in [source.span(item)])
        new_bytes = len(definition.encode("utf-8")) + len(name) * len(occurrences_for_attr)
        # Tiny constants obscure a BUILD more than they simplify it.
        if old_bytes - new_bytes < 64:
            continue
        numbers[attr] = number + 1
        used_names.add(name)
        declarations[source.span(statement)[0]].append(definition)
        edits.extend((*source.span(item), name) for _, item in occurrences_for_attr)
    edits.extend((offset, offset, "".join(lines)) for offset, lines in declarations.items())
    return source.rewrite(edits)


def remove_unused_constants(source):
    used = {node.id for node in ast.walk(source.module)
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)}
    edits = []
    for statement in source.module.body:
        if (isinstance(statement, ast.Assign) and len(statement.targets) == 1
                and isinstance(statement.targets[0], ast.Name)
                and statement.targets[0].id.startswith(CONSTANT_PREFIX)
                and statement.targets[0].id not in used
                and safe_expression(statement.value)):
            edits.append((*source.span(statement), ""))
    return source.rewrite(edits)


def inline_small_constants(source):
    uses = defaultdict(list)
    for node in ast.walk(source.module):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            uses[node.id].append(node)
    edits = []
    for statement in source.module.body:
        if (not isinstance(statement, ast.Assign) or len(statement.targets) != 1
                or not isinstance(statement.targets[0], ast.Name)
                or not statement.targets[0].id.startswith(CONSTANT_PREFIX)
                or not safe_expression(statement.value)):
            continue
        name = statement.targets[0].id
        value = source.text(statement.value)
        if not uses[name]:
            continue
        start, end = source.span(statement)
        # Input pruning can leave a formerly useful constant with one short
        # value. Keep it only when sharing the value still saves 64 bytes.
        shared_bytes = end - start + len(name) * len(uses[name])
        inline_bytes = len(value.encode("utf-8")) * len(uses[name])
        if inline_bytes - shared_bytes < 64:
            edits.extend((*source.span(use), "(" + value + ")") for use in uses[name])
    return source.rewrite(edits)


def simplify_list_expressions(source, join_expression_lists=False, preserve_selects=False):
    edits = []

    class Simplify(ast.NodeVisitor):
        def __init__(self, expressions=False):
            self.expressions = expressions

        def visit_BinOp(self, node):
            if isinstance(node.op, ast.Add):
                if isinstance(node.left, ast.List) and not node.left.elts:
                    edits.append((*source.span(node), source.text(node.right)))
                    return
                if isinstance(node.right, ast.List) and not node.right.elts:
                    edits.append((*source.span(node), source.text(node.left)))
                    return
                if (isinstance(node.left, ast.List) and isinstance(node.right, ast.List)
                        and (self.expressions or all(isinstance(value, ast.Constant)
                                                     and isinstance(value.value, (str, int, bool))
                                                     for value in node.left.elts + node.right.elts))):
                    edits.append((*source.span(node), render_list(
                        source, node.left.elts + node.right.elts, node.col_offset)))
                    return
            self.generic_visit(node)

        def visit_Call(self, node):
            value = None if preserve_selects else shared_cpu_selection(node)
            if value is not None:
                edits.append((*source.span(node), source.text(value)))
            else:
                self.generic_visit(node)

    visit = Simplify(join_expression_lists)
    for _, rule in source.rules():
        for attr in rule.keywords:
            if attr.arg in ATTRIBUTES:
                visit.visit(attr.value)
    visit = Simplify()
    for statement in source.module.body:
        if (isinstance(statement, ast.Assign) and len(statement.targets) == 1
                and isinstance(statement.targets[0], ast.Name)
                and statement.targets[0].id.startswith(CONSTANT_PREFIX)):
            visit.visit(statement.value)
    return source.rewrite(edits)


class Expand(ast.NodeTransformer):
    def __init__(self):
        self.constants = {}

    def visit_Module(self, node):
        statements = []
        for statement in node.body:
            if (isinstance(statement, ast.Assign) and len(statement.targets) == 1
                    and isinstance(statement.targets[0], ast.Name)
                    and statement.targets[0].id.startswith(CONSTANT_PREFIX)):
                self.constants[statement.targets[0].id] = self.visit(statement.value)
            else:
                statements.append(self.visit(statement))
        node.body = statements
        return node

    def visit_Name(self, node):
        return self.constants.get(node.id, node)

    def visit_Call(self, node):
        node = self.generic_visit(node)
        value = shared_cpu_selection(node)
        return value if value is not None else node

    def visit_BinOp(self, node):
        node = self.generic_visit(node)
        if not isinstance(node.op, ast.Add):
            return node
        left, right = node.left, node.right
        if isinstance(left, ast.List) and not left.elts:
            return right
        if isinstance(right, ast.List) and not right.elts:
            return left
        if isinstance(left, ast.List) and isinstance(right, ast.List):
            return ast.List(elts=left.elts + right.elts, ctx=ast.Load())
        choices = select_dictionary(left)
        if choices is not None and isinstance(right, ast.List):
            return ast.Call(func=left.func, args=[ast.Dict(
                keys=choices.keys, values=[ast.List(elts=value.elts + right.elts, ctx=ast.Load())
                                           for value in choices.values])], keywords=[])
        choices = select_dictionary(right)
        if isinstance(left, ast.List) and choices is not None:
            return ast.Call(func=right.func, args=[ast.Dict(
                keys=choices.keys, values=[ast.List(elts=left.elts + value.elts, ctx=ast.Load())
                                           for value in choices.values])], keywords=[])
        return node


def compact(text):
    original_text = text
    simplified = remove_unused_constants(Source(inline_small_constants(Source(text))))
    while True:
        # Folding adjacent literals can make select branches eligible for
        # factoring. Do both until neither changes the generated statements.
        factored = factor_selects(Source(simplified))
        result = simplify_list_expressions(Source(factored))
        if result == simplified:
            break
        simplified = result
    result = remove_unused_constants(Source(intern_attributes(Source(result))))
    original = Expand().visit(ast.parse(original_text))
    rewritten = Expand().visit(ast.parse(result))
    if ast_key(original) != ast_key(rewritten):
        raise ValueError("Compaction changed expanded BUILD statements")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="replace BUILD files after verifying expanded expressions")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("files", nargs="*", type=Path, help="default: root and generated contrib BUILD files")
    args = parser.parse_args()
    paths = args.files or [args.root / "BUILD.bazel", *sorted((args.root / "bazel/dependencies").glob("*.BUILD.bazel"))]
    total_before = total_after = files_changed = 0
    for path in paths:
        before = path.read_text()
        after = compact(before)
        total_before += len(before.encode("utf-8"))
        total_after += len(after.encode("utf-8"))
        if before != after:
            files_changed += 1
            if args.write:
                path.write_text(after)
    print(f"{files_changed}/{len(paths)} BUILD files; {total_before:,} -> {total_after:,} bytes; "
          + ("verified and written" if args.write else "verified, no files written"))


if __name__ == "__main__":
    main()

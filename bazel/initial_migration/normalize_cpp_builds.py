#!/usr/bin/env python3
"""Remove explicit cc_library defaults and redundant BUILD list expressions.

The default audit writes only to --output, outside the checkout. --write
updates BUILD files after proving both CPU configurations and every original
file's content. --exclude may be used repeatedly with an overlay basename.
"""

import argparse
import ast
from collections import Counter
from pathlib import Path
import hashlib
import json
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bazel"))

from initial_migration.compact_build import (
    Expand,
    Source,
    ast_key,
    remove_unused_constants,
    simplify_list_expressions,
)

CPP_LIBRARIES = frozenset({"cc_library", "ch_cc_library"})
CPP_COMPILERS = CPP_LIBRARIES | {"cc_binary"}
CPUS = ("x86_64", "aarch64")
# Native cc_library defaults and the matching parameters/kwargs in
# bazel/rules/ch_cc_library.bzl. In particular, never elide empty genrule,
# expand_template, filegroup, visibility, or target_compatible_with attributes.
EMPTY_CPP_DEFAULTS = frozenset({
    "srcs", "hdrs", "textual_hdrs", "additional_compiler_inputs",
    "copts", "defines", "local_defines",
    "includes", "local_includes", "features", "deps", "implementation_deps",
    "linkopts", "data",
})


def call_name(node):
    return node.func.id if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) else None


def file_digest(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def native_compile_defaults(sources, language, clickhouse_code):
    """Nonempty ch_cc_library defaults for a captured native compile group."""
    if not sources:
        return {}
    defaults = {"local_defines": "CXX_RUNTIME_DEFINES"} if language == "CXX" else {}
    if not clickhouse_code and language in ("C", "CXX"):
        defaults["copts"] = ["-w", "-std=gnu11" if language == "C" else "-std=c++23"]
    return defaults


def compile_defaults(call):
    rule = call_name(call)
    if rule not in ("ch_cc_library", "ch_grpc_cc_library"):
        return {}
    attrs = {kw.arg: kw.value for kw in call.keywords}
    sources = attrs.get("srcs")
    return native_compile_defaults(
        rule == "ch_grpc_cc_library" or sources is not None and not (isinstance(sources, ast.List) and not sources.elts),
        getattr(attrs.get("language", ast.Constant("CXX")), "value", None),
        getattr(attrs.get("clickhouse_code", ast.Constant(False)), "value", True),
    )


def removable_default(name, value, call):
    if name in ("copts", "local_defines") and name in compile_defaults(call):
        return False  # An empty override must not inherit the macro's options.
    if name == "alwayslink" or (name == "llvm_headers" and call_name(call) == "ch_cc_library"):
        return isinstance(value, ast.Constant) and value.value is False
    return (name in EMPTY_CPP_DEFAULTS and isinstance(value, ast.List)
            and not value.elts)


class CppSource(Source):
    """Restrict list expression rewrites to C++ libraries and binaries."""

    def rules(self):
        for statement, call in super().rules():
            if call_name(call) in CPP_COMPILERS:
                yield statement, call


def remove_keyword(source, kw):
    start, end = source.span(kw)
    comma = end
    while source.bytes[comma:comma + 1] in (b" ", b"\t"):
        comma += 1
    if source.bytes[comma:comma + 1] == b",":
        end = comma + 1
    return start, end, ""


def drop_defaults(source):
    edits = []
    for _, call in source.rules():
        if call_name(call) not in CPP_LIBRARIES:
            continue
        for kw in call.keywords:
            if not removable_default(kw.arg, kw.value, call):
                continue
            edits.append(remove_keyword(source, kw))
    return source.rewrite(edits)


def drop_unused_loads(source):
    used = {node.id for node in ast.walk(source.module)
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)}
    edits = []
    for statement in source.module.body:
        if not isinstance(statement, ast.Expr) or call_name(statement.value) != "load":
            continue
        call = statement.value
        if not call.args or not all(isinstance(arg, ast.Constant) and isinstance(arg.value, str)
                                    for arg in call.args):
            raise ValueError("Cannot inspect BUILD load() at line " + str(statement.lineno))
        args = [source.text(node) for node in call.args[1:] if node.value in used]
        kwargs = [source.text(kw) for kw in call.keywords if kw.arg in used]
        if len(args) + len(kwargs) == len(call.args) - 1 + len(call.keywords):
            continue
        if args or kwargs:
            replacement = "load(" + ", ".join([source.text(call.args[0]), *args, *kwargs]) + ")"
        else:
            replacement = ""
        edits.append((*source.span(statement), replacement))
    return source.rewrite(edits)


class Configured(Expand):
    """Expand generated constants and the two supported CPU selects for proof."""

    def __init__(self, cpu):
        super().__init__()
        self.cpu = "@platforms//cpu:" + cpu

    def visit_Call(self, node):
        node = self.generic_visit(node)
        if (call_name(node) == "select" and len(node.args) == 1
                and isinstance(node.args[0], ast.Dict) and not node.keywords):
            data = node.args[0]
            if all(isinstance(key, ast.Constant) for key in data.keys):
                pairs = {key.value: value for key, value in zip(data.keys, data.values)}
                keys = set(pairs)
                cpu_keys = {"@platforms//cpu:" + cpu for cpu in CPUS}
                if (keys <= cpu_keys | {"//conditions:default"} and self.cpu in pairs):
                    return pairs[self.cpu]
        if call_name(node) in CPP_LIBRARIES:
            node.keywords = [kw for kw in node.keywords if not removable_default(kw.arg, kw.value, node)]
        # buildifier formats keyword argument order; order does not affect named
        # attributes, while all ordered list elements remain untouched.
        node.keywords.sort(key=lambda kw: kw.arg or "")
        return node

    def visit_Module(self, node):
        node = super().visit_Module(node)
        # Imports unused after fully expanding generated constants are immaterial.
        # Compare active imports as sorted module/symbol pairs so buildifier can
        # combine and sort load() statements but cannot change symbol providers.
        used = {item.id for item in ast.walk(node)
                if isinstance(item, ast.Name) and isinstance(item.ctx, ast.Load)}
        imports = []
        statements = []
        for statement in node.body:
            if isinstance(statement, ast.Expr) and call_name(statement.value) == "load":
                call = statement.value
                filename = call.args[0].value
                imports.extend((filename, arg.value, arg.value)
                               for arg in call.args[1:] if arg.value in used)
                imports.extend((filename, kw.arg, kw.value.value)
                               for kw in call.keywords if kw.arg in used)
            else:
                statements.append(statement)
        node.body = statements
        # This node is for AST comparison only, not emitted to BUILD files.
        node.body.insert(0, ast.Expr(value=ast.List(
            elts=[ast.Constant(value=item) for item in sorted(imports)], ctx=ast.Load())))
        return node


def prove(before, after, path):
    for cpu in CPUS:
        lhs = Configured(cpu).visit(ast.parse(before))
        rhs = Configured(cpu).visit(ast.parse(after))
        if ast_key(lhs) != ast_key(rhs):
            left = lhs.body
            right = rhs.body
            index = next((n for n in range(min(len(left), len(right)))
                          if ast_key(left[n]) != ast_key(right[n])),
                         min(len(left), len(right)))
            detail = (
                "\nbefore: " + ast.unparse(ast.fix_missing_locations(left[index]))[:2000]
                if index < len(left) else "\nbefore: <end>"
            ) + (
                "\nafter: " + ast.unparse(ast.fix_missing_locations(right[index]))[:2000]
                if index < len(right) else "\nafter: <end>"
            )
            raise ValueError(f"{path} changes expanded {cpu} statement {index}" + detail)


def normalize(text, *, native=False):
    previous = text
    while True:
        # Removal may make another assignment or import unused. Folding lists
        # may reveal an empty default. Do not intern replacement constants.
        updated = remove_unused_constants(CppSource(previous))
        updated = simplify_list_expressions(CppSource(updated), join_expression_lists=native)
        updated = drop_defaults(CppSource(updated))
        updated = drop_unused_loads(CppSource(updated))
        if updated == previous:
            return updated
        previous = updated


def census(text):
    source = CppSource(text)
    used = {n.id for n in ast.walk(source.module)
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
    stats = Counter()
    for _, call in source.rules():
        if call_name(call) not in CPP_LIBRARIES:
            continue
        for kw in call.keywords:
            if removable_default(kw.arg, kw.value, call):
                stats["default_" + kw.arg] += 1
    for node in ast.walk(source.module):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            if any(isinstance(part, ast.List) and not part.elts for part in (node.left, node.right)):
                stats["empty_list_concatenations"] += 1
            elif isinstance(node.left, ast.List) and isinstance(node.right, ast.List):
                stats["adjacent_literal_lists"] += 1
        if call_name(node) == "load":
            stats["load_symbols"] += len(node.args) - 1 + len(node.keywords)
    for node in source.module.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id.startswith("_GENERATED_")):
            stats["generated_constants"] += 1
            if node.targets[0].id not in used:
                stats["unused_generated_constants"] += 1
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=ROOT, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--exclude", action="append", default=[],
                        help="exclude overlay basename, repeatable")
    parser.add_argument("--write", action="store_true",
                        help="apply only after all formatted files and original contents pass proof")
    parser.add_argument("files", nargs="*", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    output = args.output.resolve()
    if output == root or root in output.parents:
        parser.error("--output must be outside the source checkout")
    output.mkdir(parents=True, exist_ok=True)
    files = args.files or sorted((root / "bazel/dependencies").glob("*.BUILD.bazel"))
    files = [path for path in files if path.name not in set(args.exclude)]
    originals = {}
    stats_before = Counter()
    stats_after = Counter()
    proposed = []
    for path in files:
        path = path.resolve()
        relative = path.relative_to(root)
        native = (path.name == "BUILD.bazel" and (
            len(relative.parts) == 1 or relative.parts[0] in {"base", "src", "programs", "contrib"}
        ))
        before = path.read_text()
        after = normalize(before, native=native)
        prove(before, after, relative)
        stats_before.update(census(before))
        originals[path] = before
        destination = output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(after)
        proposed.append(destination)
    subprocess.run(["buildifier", "-config=" + str(root / ".buildifier.json"),
                    "-type=build", "-mode=fix", *map(str, proposed)],
                   cwd=output, check=True)
    report = {"inspected": len(files), "excluded": args.exclude,
              "proven_cpus": list(CPUS), "files": []}
    for path, before in originals.items():
        relative = path.relative_to(root)
        native = (path.name == "BUILD.bazel" and (
            len(relative.parts) == 1 or relative.parts[0] in {"base", "src", "programs", "contrib"}
        ))
        destination = output / relative
        after = destination.read_text()
        prove(before, after, relative)
        stats_after.update(census(after))
        if normalize(after, native=native) != after:
            raise ValueError(f"{relative}: normalization did not reach its fixed point")
        report["files"].append({
            "path": str(relative),
            "before_sha256": file_digest(before),
            "after_sha256": file_digest(after),
            "changed": before != after,
        })
    # The preview can run while others edit the checkout, but must never claim
    # a current-revision proof if a file changed while it was being inspected.
    stale = [str(path.relative_to(root)) for path, before in originals.items()
             if path.read_text() != before]
    report["stale_files"] = stale
    report["changed_files"] = sum(item["changed"] for item in report["files"])
    report["before"] = dict(sorted(stats_before.items()))
    report["after"] = dict(sorted(stats_after.items()))
    report["removed"] = {key: stats_before[key] - stats_after[key]
                         for key in sorted(stats_before | stats_after)}
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "files"}, indent=2))
    if stale:
        raise SystemExit("Checkout changed during inspection; preview is not for applying")
    if args.write:
        for item in report["files"]:
            if item["changed"]:
                (root / item["path"]).write_text((output / item["path"]).read_text())


if __name__ == "__main__":
    main()

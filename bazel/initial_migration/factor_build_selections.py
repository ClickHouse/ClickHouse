#!/usr/bin/env python3
"""Factor ordered CPU dependency/definition lists and check all selections.

Preview writes only under --output. --write requires an unchanged checkout
after every changed file passed the pre- and post-buildifier proofs.
"""

import argparse
import ast
from collections import Counter, defaultdict
from difflib import SequenceMatcher
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bazel"))
from initial_migration.compact_build import Source, ast_key, render_list, safe_expression, select_dictionary
from initial_migration.normalize_cpp_builds import Configured, call_name, removable_default, CPP_LIBRARIES

CPUS = ("@platforms//cpu:x86_64", "@platforms//cpu:aarch64")
DEFAULT = "//conditions:default"
FIELDS = {"deps", "implementation_deps", "local_defines"}
CPP_RULES = CPP_LIBRARIES | {"cc_binary", "cc_test", "llvm_runtime_objects", "nasm_objects"}


def digest(content):
    return hashlib.sha256(content.encode()).hexdigest()


def values(source):
    """Visit lists in the requested attributes and constants used only there."""
    relevant = defaultdict(set)
    all_reads = defaultdict(set)
    for node in ast.walk(source.module):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            all_reads[node.id].add(id(node))
    for _, rule in source.rules():
        if call_name(rule) not in CPP_RULES:
            continue
        for attr in rule.keywords:
            if attr.arg in FIELDS:
                yield attr.arg, attr.value, attr.col_offset
                for node in ast.walk(attr.value):
                    if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                        relevant[node.id, attr.arg].add(id(node))
    for stmt in source.module.body:
        if (isinstance(stmt, ast.Assign) and len(stmt.targets) == 1
                and isinstance(stmt.targets[0], ast.Name)):
            name = stmt.targets[0].id
            for field in FIELDS:
                if all_reads[name] and all_reads[name] == relevant[name, field]:
                    yield field, stmt.value, 0
                    break


def shared_runs(choices):
    """Positions of common blocks in every branch, in each original order."""
    lists = [[ast_key(node) for node in row.elts] for row in choices.values]
    positions = [(i,) for i in range(len(lists[0]))]
    for other in lists[1:]:
        keys = [lists[0][pos[0]] for pos in positions]
        matches = SequenceMatcher(a=keys, b=other, autojunk=False).get_matching_blocks()
        positions = [
            positions[block.a + i] + (block.b + i,)
            for block in matches for i in range(block.size)
        ]
    runs = []
    for pos in positions:
        if runs and all(current == previous + 1 for current, previous in zip(pos, runs[-1][-1])):
            runs[-1].append(pos)
        else:
            runs.append([pos])
    # A tiny interior block usually costs a second select longer than the
    # repeated literals. Prefixes/suffixes need no additional select.
    return [run for run in runs if (len(run) >= 3
            or all(index == 0 for index in run[0])
            or all(index + 1 == len(row) for index, row in zip(run[-1], lists)))]


def factor(text, stats):
    source = Source(text)
    edits = []
    for field, value, indent in values(source):
        for node in ast.walk(value):
            choices = select_dictionary(node)
            if choices is None or not safe_expression(node):
                continue
            keys = {k.value for k in choices.keys if isinstance(k, ast.Constant)}
            if not set(CPUS) <= keys <= set(CPUS) | {DEFAULT} or len(keys) != len(choices.keys):
                continue
            original = source.text(node)
            # Preserve comments and never create nested edits.
            if "#" in original or any(call_name(n) == "select"
                    for row in choices.values for item in row.elts for n in ast.walk(item)):
                stats["selects_with_comments_or_nested"] += 1
                continue
            runs = shared_runs(choices)
            if not runs:
                stats[field + "_selects_without_factorable_run"] += 1
                continue
            lists = [row.elts for row in choices.values]
            pos = [0] * len(lists)
            parts, selects = [], 0

            def selection(nextpos):
                nonlocal pos, selects
                rows = [
                    " " * (indent + 4) + source.text(key) + ": " +
                    render_list(source, row[start:end], indent + 4) + ",\n"
                    for key, row, start, end in zip(choices.keys, lists, pos, nextpos)
                ]
                parts.append("select({\n" + "".join(rows) + " " * indent + "})")
                selects += 1
                pos = nextpos

            for run in runs:
                starts = list(run[0])
                if starts != pos:
                    selection(starts)
                parts.append(render_list(source, lists[0][starts[0]:starts[0] + len(run)], indent))
                pos = [i + len(run) for i in starts]
            lengths = list(map(len, lists))
            if pos != lengths or (not selects and DEFAULT not in keys):
                # Identical CPU branches without a default still must reject
                # every unsupported CPU. An empty select preserves that.
                selection(lengths)
            replacement = "(" + " + ".join(parts) + ")"
            if len(replacement.encode()) >= len(original.encode()):
                stats[field + "_selects_without_text_savings"] += 1
                continue
            edits.append((*source.span(node), replacement))
            stats[field + "_selects_factored"] += 1
            stats[field + "_branch_literals_removed"] += (len(lists) - 1) * sum(map(len, runs))
    return source.rewrite(edits)


def join(text):
    """Join adjacent literal lists without dropping CPU-domain selects."""
    source = Source(text)
    edits = []
    class Join(ast.NodeVisitor):
        def visit_BinOp(self, node):
            if (isinstance(node.op, ast.Add) and isinstance(node.left, ast.List)
                    and isinstance(node.right, ast.List) and safe_expression(node)
                    and "#" not in source.text(node)):
                edits.append((*source.span(node), render_list(source, node.left.elts + node.right.elts, 4)))
            else:
                self.generic_visit(node)
    for _, value, _ in values(source):
        Join().visit(value)
    return source.rewrite(edits)


def normalize(text, stats=None):
    stats = stats if stats is not None else Counter()
    while True:
        changed = join(factor(text, stats))
        if changed == text:
            return text
        text = changed


def no_match(node):
    return isinstance(node, ast.Name) and node.id == "_NO_CPU_SELECT_MATCH_"


class StrictConfigured(Configured):
    """Configured comparison that preserves unsupported CPU errors.

    Expand.visit_Call intentionally collapses equal CPU selects for historical
    generator proofs. That is insufficient here because an unsupported CPU
    must keep the original select() failure when no default was specified.
    """
    def visit_Call(self, node):
        node = self.generic_visit(node)
        if call_name(node) == "select" and len(node.args) == 1 and not node.keywords:
            data = node.args[0]
            if isinstance(data, ast.Dict) and all(isinstance(key, ast.Constant) for key in data.keys):
                pairs = {key.value: value for key, value in zip(data.keys, data.values)}
                if pairs and set(pairs) <= set(CPUS) | {DEFAULT}:
                    if self.cpu in pairs:
                        return pairs[self.cpu]
                    if DEFAULT in pairs:
                        return pairs[DEFAULT]
                    return ast.Name(id="_NO_CPU_SELECT_MATCH_", ctx=ast.Load())
        if call_name(node) in CPP_LIBRARIES:
            node.keywords = [kw for kw in node.keywords if not removable_default(kw.arg, kw.value)]
        node.keywords.sort(key=lambda kw: kw.arg or "")
        return node

    def visit_BinOp(self, node):
        node = self.generic_visit(node)
        if isinstance(node.op, ast.Add):
            if no_match(node.left) or no_match(node.right):
                return ast.Name(id="_NO_CPU_SELECT_MATCH_", ctx=ast.Load())
            if isinstance(node.left, ast.List) and not node.left.elts:
                return node.right
            if isinstance(node.right, ast.List) and not node.right.elts:
                return node.left
            if isinstance(node.left, ast.List) and isinstance(node.right, ast.List):
                return ast.List(elts=node.left.elts + node.right.elts, ctx=ast.Load())
        return node


def prove(before, after, path):
    for cpu in ("x86_64", "aarch64", "unsupported"):
        lhs = StrictConfigured(cpu).visit(ast.parse(before))
        rhs = StrictConfigured(cpu).visit(ast.parse(after))
        if ast_key(lhs) != ast_key(rhs):
            left, right = lhs.body, rhs.body
            index = next((n for n in range(min(len(left), len(right))) if ast_key(left[n]) != ast_key(right[n])),
                         min(len(left), len(right)))
            old = ast.unparse(ast.fix_missing_locations(left[index]))[:2000] if index < len(left) else "<end>"
            new = ast.unparse(ast.fix_missing_locations(right[index]))[:2000] if index < len(right) else "<end>"
            raise ValueError(f"{path}: {cpu} statement {index} differs\nOLD {old}\nNEW {new}")


def run(root, files):
    """Generator pass: factor only after all other BUILD normalizations."""
    stats = Counter()
    for path in sorted(set(files)):
        if not path.is_file():
            continue
        before = path.read_text()
        after = normalize(before, stats)
        if after != before:
            prove(before, after, path.relative_to(root))
            if normalize(after) != after:
                raise ValueError(f"{path}: normalization did not reach a fixed point")
            path.write_text(after)
    return dict(stats)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("files", type=Path, nargs="*")
    args = parser.parse_args()
    root, output = args.root.resolve(), args.output.resolve()
    if output == root or root in output.parents:
        parser.error("--output must be outside the source checkout")
    inventory = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                                        cwd=root).decode().split("\0")
    files = args.files or [root / name for name in inventory if name and (
        name == "BUILD.bazel" or
        (Path(name).name == "BUILD.bazel" and name.split("/")[0] in {"base", "src", "programs", "contrib"}) or
        (name.startswith("bazel/dependencies/") and name.endswith(".BUILD.bazel")))]
    files = sorted(set(path.resolve() for path in files if path.is_file()))
    originals, stats, proposed = {}, Counter(), []
    output.mkdir(parents=True, exist_ok=True)
    for path in files:
        relative = path.relative_to(root)
        before = path.read_text()
        after = normalize(before, stats)
        if after == before:
            continue
        prove(before, after, relative)
        destination = output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(after)
        originals[path] = before
        proposed.append(destination)
    if proposed:
        subprocess.run(["buildifier", "-config=" + str(root / ".buildifier.json"), "-type=build", "-mode=fix",
                        *map(str, proposed)], cwd=output, check=True)
    report = {"inspected": len(files), "configs": ["x86_64", "aarch64", "unsupported"],
              "stats": dict(stats), "files": []}
    for path, before in originals.items():
        relative = path.relative_to(root)
        after = (output / relative).read_text()
        prove(before, after, relative)
        second = normalize(after)
        if second != after:
            raise ValueError(f"{relative}: not at normalization fixed point after buildifier")
        report["files"].append({"path": str(relative), "before_sha256": digest(before), "after_sha256": digest(after),
                                "bytes_removed": len(before.encode()) - len(after.encode()),
                                "lines_removed": len(before.splitlines()) - len(after.splitlines())})
    report["stale_files"] = [str(path.relative_to(root)) for path, before in originals.items()
                             if not path.exists() or path.read_text() != before]
    report["bytes_removed"] = sum(row["bytes_removed"] for row in report["files"])
    report["lines_removed"] = sum(row["lines_removed"] for row in report["files"])
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "files"}, indent=2))
    if args.write:
        if report["stale_files"]:
            raise SystemExit("Files changed while being inspected; cannot apply the preview")
        for path in originals:
            path.write_text((output / path.relative_to(root)).read_text())


if __name__ == "__main__":
    main()

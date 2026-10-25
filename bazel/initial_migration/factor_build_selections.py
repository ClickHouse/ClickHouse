#!/usr/bin/env python3
"""Factor ordered C++ BUILD lists and check all selections.

Preview writes only under --output. --write requires an unchanged checkout
after every changed file passed the pre- and post-buildifier proofs.
"""

import argparse
import ast
from collections import Counter, defaultdict
import copy
from difflib import SequenceMatcher
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tokenize

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bazel"))
from initial_migration.compact_build import Source, ast_key, render_list, safe_expression, select_dictionary
from initial_migration.normalize_cpp_builds import Configured, call_name, removable_default, CPP_LIBRARIES

CPUS = ("@platforms//cpu:x86_64", "@platforms//cpu:aarch64")
DEFAULT = "//conditions:default"
FIELDS = {"deps", "implementation_deps", "local_defines", "hdrs", "textual_hdrs", "local_includes"}
CPP_RULES = CPP_LIBRARIES | {"cc_binary", "cc_test", "ch_cc_test", "llvm_runtime_objects", "nasm_objects"}


def digest(content):
    return hashlib.sha256(content.encode()).hexdigest()


def values(source, guards=False):
    """Visit typed lists and constants used only in those typed attributes."""
    relevant = defaultdict(set)
    guarded_reads = defaultdict(set)
    all_reads = defaultdict(set)
    witnesses = {}
    if guards:
        configured = StrictConfigured("unsupported", collapse_failed=False).visit(copy.deepcopy(source.module))
        for stmt in configured.body:
            if not isinstance(stmt, ast.Expr) or call_name(stmt.value) not in CPP_RULES:
                continue
            name = next((kw.value.value for kw in stmt.value.keywords
                         if kw.arg == "name" and isinstance(kw.value, ast.Constant)), None)
            if name is not None:
                witnesses[name] = {kw.arg for kw in stmt.value.keywords if no_match(kw.value)}
    for node in ast.walk(source.module):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            all_reads[node.id].add(id(node))
    for _, rule in source.rules():
        if call_name(rule) not in CPP_RULES:
            continue
        name = next((kw.value.value for kw in rule.keywords
                     if kw.arg == "name" and isinstance(kw.value, ast.Constant)), None)
        for attr in rule.keywords:
            if attr.arg in FIELDS:
                failed = witnesses.get(name, set())
                # Two attributes must never use one another as permission to
                # remove both CPU checks. Prefer a witness we never rewrite.
                anchor = min(failed, key=lambda field: (field in FIELDS, field)) if failed else None
                guarded = anchor is not None and anchor != attr.arg
                yield attr.arg, attr, attr.col_offset, guarded
                for node in ast.walk(attr.value):
                    if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                        relevant[node.id, attr.arg].add(id(node))
                        if guarded:
                            guarded_reads[node.id, attr.arg].add(id(node))
    for stmt in source.module.body:
        if (isinstance(stmt, ast.Assign) and len(stmt.targets) == 1
                and isinstance(stmt.targets[0], ast.Name)):
            name = stmt.targets[0].id
            for field in FIELDS:
                if all_reads[name] and all_reads[name] == relevant[name, field]:
                    yield field, stmt, 0, all_reads[name] == guarded_reads[name, field]
                    break


def list_nodes(node):
    """Do not recurse into list elements: repository_path() returns a string."""
    yield node
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        yield from list_nodes(node.left)
        yield from list_nodes(node.right)
    elif call_name(node) == "select" and node.args and isinstance(node.args[0], ast.Dict):
        for value in node.args[0].values:
            yield from list_nodes(value)


def cpu_choices(node):
    if call_name(node) != "select" or len(node.args) != 1 or node.keywords:
        return {}
    choices = node.args[0]
    if not isinstance(choices, ast.Dict) or not all(
            isinstance(key, ast.Constant) and key.value in (*CPUS, DEFAULT) for key in choices.keys):
        return {}
    return {key.value: value for key, value in zip(choices.keys, choices.values)}


def fold_single_cpu_lists(source):
    """Keep the configured list on the only CPU allowed by target_compatible_with."""
    constants = {}
    reads = Counter(n.id for n in ast.walk(source.module) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load))
    guarded = defaultdict(list)
    edits = []

    def fold(value, cpu, guards):
        end = -1
        for node in list_nodes(value):
            choices = cpu_choices(node)
            if cpu not in choices or (DEFAULT not in choices and
                                       (DEFAULT in guards or not set(CPUS) <= choices.keys())):
                continue
            # An unchosen glob() or function can still run during package loading.
            if any(not isinstance(row, ast.List) or row.elts for key, row in choices.items() if key != cpu):
                continue
            start, stop = source.span(node)
            if start >= end and "#" not in source.text(node):
                edits.append((start, stop, source.text(choices[cpu])))
                end = stop

    for stmt in source.module.body:
        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
            constants[stmt.targets[0].id] = stmt.value
        if not isinstance(stmt, ast.Expr) or call_name(stmt.value) not in CPP_LIBRARIES | {"nasm_objects"}:
            continue
        rule = stmt.value
        attrs = {kw.arg: kw.value for kw in rule.keywords}
        guard = attrs.get("target_compatible_with")
        if isinstance(guard, ast.Name):
            guard = constants.get(guard.id)
        try:
            guards = {key: ast.literal_eval(value) for key, value in cpu_choices(guard).items()}
        except (TypeError, ValueError):
            continue
        cpus = [key for key in CPUS if guards.get(key) == []]
        if len(cpus) != 1 or any(value != ["@platforms//:incompatible"]
                                 for key, value in guards.items() if key != cpus[0]):
            continue
        cpu, = cpus
        for field in (FIELDS | {"srcs", "private_hdrs", "copts", "includes", "defines"}) & attrs.keys():
            fold(attrs[field], cpu, guards)
            for node in list_nodes(attrs[field]):
                if isinstance(node, ast.Name) and node.id in constants:
                    guarded[node.id].append((cpu, guards))
    # Other uses of a constant may lack this guard or select the other CPU.
    for name, uses in guarded.items():
        if len(uses) == reads[name] and all(use == uses[0] for use in uses):
            cpu, guards = uses[0]
            chosen = cpu_choices(constants[name]).get(cpu)
            # A nonempty list also preserves ch_cc_library's pre-analysis truth test for srcs.
            if isinstance(chosen, ast.List) and chosen.elts and all(
                    isinstance(v, ast.Constant) and isinstance(v.value, str) for v in chosen.elts):
                fold(constants[name], cpu, guards)
    return source.rewrite(edits)


def list_parts(node):
    """Flatten only additions in an attribute that requires a list."""
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return list_parts(node.left) + list_parts(node.right)
    return [node]


def typed_list(node):
    """Reject tuple/string arithmetic and opaque functions in list additions."""
    if isinstance(node, ast.List) or isinstance(node, ast.Name):
        return True
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return typed_list(node.left) and typed_list(node.right)
    return call_name(node) in {"select", "glob"}


def canonical_list(node):
    """Represent associative list additions in order, joining literal siblings."""
    if not typed_list(node):
        return node
    parts = []
    for item in list_parts(node):
        if parts and isinstance(item, ast.List) and isinstance(parts[-1], ast.List):
            parts[-1] = ast.List(elts=parts[-1].elts + item.elts, ctx=ast.Load())
        else:
            parts.append(item)
    result = parts[0]
    for item in parts[1:]:
        result = ast.BinOp(left=result, op=ast.Add(), right=item)
    return result


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
    for field, attribute, indent, guarded in values(source, guards=True):
        edited = []
        for node in list_nodes(attribute.value):
            if call_name(node) != "select" or len(node.args) != 1 or node.keywords:
                continue
            choices = node.args[0]
            if not isinstance(choices, ast.Dict):
                continue
            start, end = source.span(node)
            if any(first <= start and end <= last for first, last in edited):
                continue
            keys = {k.value for k in choices.keys if isinstance(k, ast.Constant)}
            if not set(CPUS) <= keys <= set(CPUS) | {DEFAULT} or len(keys) != len(choices.keys):
                continue
            original = source.text(node)
            if "#" in original:
                stats["selects_with_comments_or_nested"] += 1
                continue
            branches = {k.value: v for k, v in zip(choices.keys, choices.values)}
            shared = branches[CPUS[0]]
            if (typed_list(shared) and ast_key(shared) == ast_key(branches[CPUS[1]])
                    and (guarded or (DEFAULT in branches and ast_key(branches[DEFAULT]) == ast_key(shared)))):
                replacement = source.text(shared)
                if isinstance(shared, ast.BinOp):
                    replacement = "(" + replacement + ")"
                edits.append((start, end, replacement))
                edited.append((start, end))
                stats[field + "_equal_cpu_selects_removed"] += 1
                continue
            choices = select_dictionary(node)
            if choices is None or not safe_expression(node):
                continue
            # Preserve comments and never create nested edits.
            if any(call_name(n) == "select"
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
            edited.append((start, end))
            stats[field + "_selects_factored"] += 1
            stats[field + "_branch_literals_removed"] += (len(lists) - 1) * sum(map(len, runs))
    return source.rewrite(edits)


def join(text):
    """Flatten typed list additions; leave strings, tuples, and comments alone."""
    source = Source(text)
    edits = []
    for _, attr, indent, _ in values(source):
        node = attr.value
        if not isinstance(node, ast.BinOp) or not typed_list(node) or "#" in source.text(node):
            continue
        wanted = canonical_list(node)
        if ast_key(wanted) == ast_key(node):
            continue
        chunks, pending = [], []
        for item in list_parts(node):
            if isinstance(item, ast.List):
                pending.extend(item.elts)
            else:
                if pending:
                    chunks.append(render_list(source, pending, indent))
                    pending = []
                chunks.append(source.text(item))
        if pending:
            chunks.append(render_list(source, pending, indent))
        if chunks:
            edits.append((*source.span(node), "(" + " + ".join(chunks) + ")"))
    return source.rewrite(edits)


def normalize(text, stats=None):
    stats = stats if stats is not None else Counter()
    while True:
        changed = remove_parens(join(factor(text, stats)))
        if changed == text:
            return text
        text = changed


def source_list_key(module):
    """Compare srcs sums by operand order; leave all other expressions intact."""
    class SourceLists(ast.NodeTransformer):
        def visit_BinOp(self, node):
            node = self.generic_visit(node)
            if (isinstance(node.op, ast.Add) and
                    all(isinstance(part, ast.List) or call_name(part) == "glob"
                        or select_dictionary(part) is not None for part in list_parts(node))):
                return canonical_list(node)
            return node

    for keyword in ast.walk(module):
        if isinstance(keyword, ast.keyword) and keyword.arg == "srcs":
            keyword.value = SourceLists().visit(keyword.value)
    return ast_key(module)


def remove_parens(text, key=ast_key):
    """Remove grouping only when the compared BUILD expressions agree."""
    source = Source(text)
    tokens = list(tokenize.generate_tokens(io.StringIO(text).readline))
    pairs, stack = [], []
    previous = None
    ignorable = {tokenize.ENCODING, tokenize.NL, tokenize.NEWLINE, tokenize.COMMENT,
                 tokenize.INDENT, tokenize.DEDENT, tokenize.ENDMARKER}
    for index, token in enumerate(tokens):
        if token.type in ignorable:
            continue
        if token.type == tokenize.OP and token.string == "(":
            # Function calls cannot lose their parentheses. Calls such as
            # select({...}) and glob([...]) stay intact.
            called = previous is not None and (
                previous.type == tokenize.NAME or previous.string in {")", "]", "}"})
            stack.append((index, called))
        elif token.type == tokenize.OP and token.string == ")" and stack:
            first, called = stack.pop()
            if not called:
                pairs.append((first, index))
        previous = token
    if not pairs:
        return text
    wanted = key(source.module)
    # AST offsets count bytes, while tokenize columns count characters.
    lines = text.splitlines(keepends=True)
    def pos(where):
        row, column = where
        return source.starts[row - 1] + len(lines[row - 1][:column].encode())
    removed = []
    for first, last in pairs:
        proposed = removed + [
            (pos(tokens[first].start), pos(tokens[first].end), ""),
            (pos(tokens[last].start), pos(tokens[last].end), ""),
        ]
        candidate = source.rewrite(proposed)
        try:
            parsed = ast.parse(candidate)
        except SyntaxError:
            continue
        if key(parsed) == wanted:
            removed = proposed
    return source.rewrite(removed)


def no_match(node):
    return isinstance(node, ast.Name) and node.id == "_NO_CPU_SELECT_MATCH_"


class StrictConfigured(Configured):
    """Configured comparison that preserves unsupported CPU errors.

    Expand.visit_Call intentionally collapses equal CPU selects for historical
    generator proofs. That is insufficient here because an unsupported CPU
    must keep the original select() failure when no default was specified.
    """
    def __init__(self, cpu, collapse_failed=True):
        super().__init__(cpu)
        self.collapse_failed = collapse_failed

    def visit_Module(self, node):
        node = super().visit_Module(node)
        proof = Source.__new__(Source)
        proof.module = node
        for _, attr, _, _ in values(proof):
            attr.value = canonical_list(attr.value)
        return node

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
            node.keywords = [kw for kw in node.keywords if not removable_default(kw.arg, kw.value, node)]
        if (self.collapse_failed and self.cpu == "@platforms//cpu:unsupported"
                and call_name(node) in CPP_RULES
                and any(no_match(kw.value) for kw in node.keywords)):
            # An unsupported CPU cannot analyze this target through any
            # attribute. Keep rule type/name for comparison, not failed values.
            node.keywords = [kw for kw in node.keywords if kw.arg == "name"]
            node.keywords.append(ast.keyword(arg="_failed_cpu_select",
                                             value=ast.Constant(value=True)))
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

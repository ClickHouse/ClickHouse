#!/usr/bin/env python3
"""Replace duplicated external CMake configuration outputs with configured rules.

The generated file's contents remain selectable. Its label need not contain the
CPU: Bazel already writes artifacts below the target configuration's bin dir.
Run with --write only after the complete in-memory verification succeeds.
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

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from initial_migration.compact_build import Source, ast_key

CPUS = ("x86_64", "aarch64")
SETTINGS = {cpu: "@platforms//cpu:" + cpu for cpu in CPUS}
CONFIG = re.compile(r"(?<!bazel/)config/(x86_64|aarch64)/(contrib/[^/\s\"']+)(?=/|$)")
KINDS = {"expand_template", "copy_file", "write_file", "genrule"}
CONFIGURABLE = {"template", "substitutions", "src", "srcs", "content", "tools", "cmd"}
ROOT = Path(__file__).resolve().parents[2]


def attributes(call):
    return {kw.arg: kw.value for kw in call.keywords}


def text_constant(node):
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def output_name(call):
    fields = attributes(call)
    if "out" in fields:
        return text_constant(fields["out"])
    outs = fields.get("outs")
    if isinstance(outs, ast.List) and len(outs.elts) == 1:
        return text_constant(outs.elts[0])
    return None


def archless_output(output):
    result = CONFIG.sub(lambda m: "config/" + m[2], output)
    # These source templates genuinely differ. Only their configured output
    # directory is shared; select() retains the two original templates.
    for cpu in CPUS:
        result = result.replace(
            "config/contrib/jemalloc-cmake/include_linux_" + cpu + "/",
            "config/contrib/jemalloc-cmake/include_linux/",
        )
    return result


class ChooseCpu(ast.NodeTransformer):
    def __init__(self, cpu):
        self.setting = SETTINGS[cpu]

    def visit_Call(self, node):
        if (isinstance(node.func, ast.Name) and node.func.id == "select"
                and len(node.args) == 1 and isinstance(node.args[0], ast.Dict)):
            branches = node.args[0]
            for key, value in zip(branches.keys, branches.values, strict=True):
                if text_constant(key) == self.setting:
                    return self.visit(copy.deepcopy(value))
        return self.generic_visit(node)


class MapConstants(ast.NodeTransformer):
    def __init__(self, migration, owner):
        self.migration = migration
        self.owner = owner

    def visit_Constant(self, node):
        if isinstance(node.value, str):
            return ast.copy_location(
                ast.Constant(self.migration.value(node.value, self.owner)), node)
        return node


def collapse_equal_cpu_selects(text):
    """Remove CPU selects whose complete expressions became equal."""
    while True:
        source = Source(text)
        edits = []
        for node in ast.walk(source.module):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id == "select" and len(node.args) == 1
                    and not node.keywords and isinstance(node.args[0], ast.Dict)):
                continue
            choices = node.args[0]
            keys = {text_constant(key) for key in choices.keys}
            if keys != set(SETTINGS.values()):
                continue
            if len({ast_key(value) for value in choices.values}) != 1:
                continue
            lower, upper = source.span(node)
            # Process outermost selects first. A subsequent pass handles their
            # children, avoiding overlapping byte edits on nested selects.
            if any(start <= lower and upper <= end for start, end, _ in edits):
                continue
            edits.append((lower, upper, source.text(choices.values[0])))
        if not edits:
            return text
        text = source.rewrite(edits)


def key_for_cpu(node, cpu, mapper=None):
    result = copy.deepcopy(node)
    if mapper:
        result = mapper.visit(result)
    return ast_key(ChooseCpu(cpu).visit(result))


def configured_rule_key(call, cpu):
    """Compare expand_template inputs, treating a dict union as one substitution map."""
    def value(expr):
        expr = ChooseCpu(cpu).visit(copy.deepcopy(expr))
        if isinstance(expr, ast.Dict):
            return dict(zip(map(text_constant, expr.keys), map(ast_key, expr.values), strict=True))
        if isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.BitOr):
            return value(expr.left) | value(expr.right)
        return ast_key(expr)

    return call.func.id, {name: value(expr) for name, expr in attributes(call).items()}


class Migration:
    def __init__(self, root, *, source_overrides=None):
        self.root = root
        self.source_overrides = source_overrides or {}
        self.families = defaultdict(dict)
        self.owners = {}
        self.directories = set()
        self.outputs = {}
        self.rule_labels = {}
        self.counts = Counter()

    def collect(self):
        for path in sorted((self.root / "bazel/dependencies").glob("*.BUILD.bazel")):
            owner = path.name.removesuffix(".BUILD.bazel")
            # @clickhouse_llvm_project owns a separate llvm-tblgen/runtime
            # graph under gen/<cpu>. This pass migrates CMake configure_file.
            if owner == "clickhouse_llvm_project":
                continue
            source = Source(self.source_overrides[path] if path in self.source_overrides else path.read_text())
            self.owners[path] = owner
            for statement, call in source.rules():
                if not isinstance(call.func, ast.Name) or call.func.id not in KINDS:
                    continue
                output = output_name(call)
                if not output:
                    continue
                match = CONFIG.match(output)
                if not match:
                    continue
                cpu, directory = match.groups()
                shared = archless_output(output)
                family = self.families[owner, shared]
                if cpu in family:
                    raise ValueError("Duplicate output: " + owner + "//:" + output)
                family[cpu] = (path, source, statement, call, output)
                self.directories.add(directory)

        for (owner, shared), family in self.families.items():
            if set(family) != set(CPUS):
                raise ValueError("No matching configuration output: " + str((owner, shared, list(family))))
            kinds = {row[3].func.id for row in family.values()}
            if len(kinds) != 1:
                raise ValueError("Rule kind changed across CPUs: " + str((owner, shared, kinds)))
            suffix = shared.split("/", 3)[-1]
            name = "configure_" + re.sub(r"[^a-zA-Z0-9_]+", "_", suffix).strip("_")
            self.counts[(next(iter(kinds)), "paired")] += 1
            for cpu, row in family.items():
                call, output = row[3], row[4]
                old_name = text_constant(attributes(call)["name"])
                if not old_name:
                    raise ValueError("Nonliteral configuration rule name: " + owner + "//:" + output)
                self.outputs[owner, output] = shared
                self.rule_labels[owner, old_name] = name
        for owner in set(self.owners.values()):
            names = [name for (candidate, _), name in self.rule_labels.items() if candidate == owner]
            if len(set(names)) * 2 != len(names):
                raise ValueError("Colliding shared configuration name in " + owner)

        for path in self.source_overrides:
            owner = self.owners[path]
            actual = {}
            for _, call in Source(path.read_text()).rules():
                if call.func.id in KINDS and (text_constant(attributes(call)["name"]) or "").startswith("configure_"):
                    out = output_name(call)
                    if out in actual:
                        raise ValueError(f"Duplicate configured output in {path}: {out}")
                    actual[out] = call
            families = {shared: family for (repo, shared), family in self.families.items() if repo == owner}
            if not families or set(families) != actual.keys():
                raise ValueError("CMake outputs differ from the maintained BUILD file: " + str(path))
            for shared, family in families.items():
                expected = ast.parse(self.render(owner, shared, family)).body[0].value
                for cpu in CPUS:
                    if configured_rule_key(expected, cpu) != configured_rule_key(actual[shared], cpu):
                        raise ValueError(f"Update {path} for the CMake configuration of {shared} on {cpu}")

    def value(self, value, owner):
        # Change directories only when collect() found declared external outputs
        # at that exact CMake directory. Do not rename tracked bazel/config files.
        result = CONFIG.sub(
            lambda match: "config/" + match[2] if match[2] in self.directories else match[0],
            value,
        )
        for cpu in CPUS:
            result = result.replace(
                "config/contrib/jemalloc-cmake/include_linux_" + cpu + "/",
                "config/contrib/jemalloc-cmake/include_linux/",
            )
        if result.startswith(":") and (owner, result[1:]) in self.rule_labels:
            result = ":" + self.rule_labels[owner, result[1:]]
        match = re.fullmatch(r"@([^/]+)//:([^/]+)", result)
        if match and (match[1], match[2]) in self.rule_labels:
            result = "@" + match[1] + "//:" + self.rule_labels[match[1], match[2]]
        return result

    def node_text(self, source, node, owner):
        edits = []
        for child in ast.walk(node):
            old = text_constant(child)
            if old is None:
                continue
            new = self.value(old, owner)
            if new != old:
                edits.append((*source.span(child), json.dumps(new, ensure_ascii=False)))
        if not edits:
            return source.text(node)
        start, end = source.span(node)
        raw = source.bytes[start:end]
        offset = 0
        chunks = []
        for lower, upper, replacement in sorted(edits):
            lower -= start
            upper -= start
            chunks.extend((raw[offset:lower], replacement.encode()))
            offset = upper
        chunks.append(raw[offset:])
        return b"".join(chunks).decode()

    def render(self, owner, shared, family):
        first, second = (family[cpu] for cpu in CPUS)
        old = [attributes(row[3]) for row in (first, second)]
        if set(old[0]) != set(old[1]):
            raise ValueError("Different configuration attributes: " + str((owner, shared)))
        mapper = MapConstants(self, owner)
        fields = []
        name = self.rule_labels[owner, text_constant(old[0]["name"])]
        for field in old[0]:
            if field == "name":
                value = json.dumps(name)
            elif field == "out":
                value = json.dumps(shared)
            elif field == "outs":
                value = "[" + json.dumps(shared) + "]"
            else:
                text = [self.node_text(row[1], attrs[field], owner)
                        for row, attrs in zip((first, second), old, strict=True)]
                if key_for_cpu(old[0][field], CPUS[0], mapper) == key_for_cpu(old[1][field], CPUS[1], mapper):
                    value = text[0]
                else:
                    if field not in CONFIGURABLE:
                        raise ValueError("Cannot configure " + field + " in " + owner + "//:" + name)
                    self.counts[("selected", field)] += 1
                    value = ("select({\n" + "".join(
                        "        " + json.dumps(SETTINGS[cpu]) + ": " + item + ",\n"
                        for cpu, item in zip(CPUS, text, strict=True)) + "    })")
            fields.append("    " + field + " = " + value + ",\n")
        rendered = first[3].func.id + "(\n" + "".join(fields) + ")"
        new = attributes(ast.parse(rendered).body[0].value)
        for cpu, old_attrs in zip(CPUS, old, strict=True):
            for field, node in old_attrs.items():
                expected = key_for_cpu(node, cpu, mapper)
                if field == "name":
                    expected = ast_key(ast.Constant(name))
                if key_for_cpu(new[field], cpu) != expected:
                    raise ValueError("Configuration content changed: " + str((owner, shared, cpu, field)))
        return rendered

    def starlark(self, path, source):
        owner = self.owners.get(path)
        edits = []
        removed = []
        for (candidate, shared), family in self.families.items():
            if candidate != owner:
                continue
            for cpu in CPUS:
                row = family[cpu]
                lower, upper = source.span(row[2])
                removed.append((lower, upper))
                replacement = self.render(owner, shared, family) if cpu == CPUS[0] else ""
                edits.append((lower, upper, replacement))
        for node in ast.walk(source.module):
            value = text_constant(node)
            if value is None:
                continue
            lower, upper = source.span(node)
            if any(start <= lower < end for start, end in removed):
                continue
            mapped = self.value(value, owner)
            if mapped != value:
                edits.append((lower, upper, json.dumps(mapped, ensure_ascii=False)))
        if not edits:
            return None
        new = collapse_equal_cpu_selects(source.rewrite(edits))
        target = Source(new)
        self.verify_rules(path, source, target)
        self.counts[("starlark", "files")] += 1
        self.counts[("starlark", "constant_edits")] += len(edits) - len(removed)
        return new

    def verify_rules(self, path, source, target):
        owner = self.owners.get(path)
        mapper = MapConstants(self, owner)
        old_rules = {}
        for _, call in source.rules():
            name = text_constant(attributes(call)["name"])
            if (owner, name) not in self.rule_labels:
                old_rules[name] = call
        new_rules = {}
        new_names = {name for (candidate, _), name in self.rule_labels.items() if candidate == owner}
        for _, call in target.rules():
            name = text_constant(attributes(call)["name"])
            if name not in new_names:
                new_rules[name] = call
        if set(old_rules) != set(new_rules):
            raise ValueError("Native rule set changed: " + str(path))
        for name, call in old_rules.items():
            for cpu in CPUS:
                if key_for_cpu(call, cpu, mapper) != key_for_cpu(new_rules[name], cpu):
                    raise ValueError("Rule changed outside exact configured labels: " + str((path, name, cpu)))
            self.counts[("proof", "unchanged_rules")] += 1

    def metadata(self):
        path = self.root / "bazel/initial_migration/any2bazel.json"
        data = json.loads(path.read_text())
        # A source identity describes the translation unit independently of its
        # CPU. Preserve both reference sources; share the Bazel source identity.
        identities = {}
        for (owner, output), shared in self.outputs.items():
            _, cpu, relative = output.split("/", 2)
            key = "@build/" + cpu + "/" + relative
            identities[key] = "@build/" + shared.removeprefix("config/")
        for field in ("source_map", "include_map"):
            new = {}
            for row in data["ignore"][field]:
                original, identity = row["from"], row["to"]
                if field == "source_map" and identity in identities:
                    original = self.value(original, None)
                    identity = identities[identity]
                elif field == "include_map":
                    match = re.fullmatch(
                        r"(bazel-out/k8-opt/bin/external/\+contrib\+([^/]+)/config)/(x86_64|aarch64)",
                        original,
                    )
                    if match and match[2] in set(self.owners.values()) and any(
                            owner == match[2] for owner, _ in self.outputs):
                        original = match[1]
                        identity = "@build"
                    # include_map compares one CMake/Bazel target CPU at a
                    # time. Use the same identity for the remaining CMake
                    # build directories and their configured Bazel counterparts.
                    identity = re.sub(r"^@build/(?:x86_64|aarch64)(?=/|$)", "@build", identity)
                if original in new and new[original] != identity:
                    raise ValueError("Conflicting any2bazel identities: " + str((original, new[original], identity)))
                new[original] = identity
            data["ignore"][field] = [{"from": key, "to": value} for key, value in new.items()]
        return path, json.dumps(data, indent=2) + "\n"

    def run(self, write=False):
        self.collect()
        changed = {}
        tracked = subprocess.check_output(
            ["git", "ls-files", "-z", "--", "*.bazel", "*.bzl"], cwd=self.root,
        ).decode().split("\0")
        for relative in tracked:
            if not relative:
                continue
            path = self.root / relative
            if path in self.source_overrides or not path.is_file():
                continue
            text = path.read_text()
            if not CONFIG.search(text) and "configure_x86_64_" not in text and "configure_aarch64_" not in text:
                continue
            result = self.starlark(path, Source(text))
            if result is not None:
                changed[path] = result
        path, text = self.metadata()
        if text != path.read_text():
            changed[path] = text
        if write:
            for path, text in changed.items():
                path.write_text(text)
        print(json.dumps({
            "write": write,
            "outputs": len(self.outputs),
            "shared_rules": len(self.families),
            "counts": {"/".join(key): value for key, value in sorted(self.counts.items())},
            "files": [str(path.relative_to(self.root)) for path in sorted(changed)],
        }, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--root", type=Path, default=ROOT)
    opts = parser.parse_args()
    Migration(opts.root.resolve()).run(write=opts.write)


if __name__ == "__main__":
    main()

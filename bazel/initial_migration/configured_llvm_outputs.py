#!/usr/bin/env python3
"""Use configured output labels for ClickHouse's embedded LLVM generators.

This applies only to @clickhouse_llvm_project. @llvm-project tools, compiler
files, libc headers, and sysroot generation remain owned by the LLVM toolchain.
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
from initial_migration.configured_external_outputs import (
    CPUS, SETTINGS, ChooseCpu, attributes, collapse_equal_cpu_selects, key_for_cpu, text_constant,
)

ROOT = Path(__file__).resolve().parents[2]
RAW = "clickhouse_llvm_project"
PREFIX = "contrib/llvm-project"
GENERATED = re.compile(r"gen/(x86_64|aarch64)(?:/([A-Za-z0-9_+./-]+))?")
KINDS = {"copy_file", "expand_template", "write_file", "genrule"}
CONFIGURABLE = {"src", "template", "substitutions", "content", "srcs", "cmd", "tools"}
ACTUAL_ROOT = "bazel-out/k8-opt/bin/external/+contrib+" + RAW + "/"


def raw_repository_call(node):
    return (
        isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id == "repository_path" and len(node.args) == 1
        and text_constant(node.args[0]) == PREFIX
    )


class MapLLVM(ast.NodeTransformer):
    def __init__(self, migration, raw=False):
        self.migration = migration
        self.raw = raw

    def visit_BinOp(self, node):
        # repository_path() + "/gen/<cpu>/..." occurs in the ISA-L NASM
        # options and runtime copts. A suffix alone does not name a repository.
        if not self.raw and any(raw_repository_call(child) for child in ast.walk(node)):
            return MapLLVM(self.migration, raw=True).visit(node)
        return self.generic_visit(node)

    def visit_Constant(self, node):
        if isinstance(node.value, str):
            mapped = self.migration.value(node.value, self.raw)
            if mapped != node.value:
                return ast.copy_location(ast.Constant(mapped), node)
        return node


class BoundConstants(ast.NodeTransformer):
    def __init__(self, definitions):
        self.definitions = definitions
        self.visiting = set()

    def visit_Name(self, node):
        if node.id not in self.definitions or node.id in self.visiting:
            return node
        self.visiting.add(node.id)
        result = self.visit(copy.deepcopy(self.definitions[node.id]))
        self.visiting.remove(node.id)
        return result


class Migration:
    def __init__(self, root):
        self.root = root
        self.path = root / "bazel/dependencies/clickhouse_llvm_project.BUILD.bazel"
        self.outputs = {}
        self.directories = set()
        self.families = defaultdict(dict)
        self.rename_rules = {}
        self.counts = Counter()

    def collect(self, source):
        existing_names = set()
        for statement, call in source.rules():
            fields = attributes(call)
            name = text_constant(fields["name"])
            existing_names.add(name)
            if not isinstance(call.func, ast.Name) or call.func.id not in KINDS:
                continue
            if "out" in fields:
                outs = [text_constant(fields["out"])]
            elif isinstance(fields.get("outs"), ast.List):
                outs = [text_constant(node) for node in fields["outs"].elts]
            else:
                continue
            if not outs or any(out is None for out in outs):
                continue
            match = GENERATED.fullmatch(outs[0])
            if not match or not match[2]:
                continue
            cpu, relative = match.groups()
            family = self.families[relative]
            if cpu in family:
                raise ValueError("Duplicate LLVM generated file: " + str((relative, cpu)))
            family[cpu] = (statement, call, outs)
            for out in outs:
                match = GENERATED.fullmatch(out)
                if not match or match[1] != cpu or not match[2]:
                    raise ValueError("LLVM genrule mixes configured and unrelated outputs: " + str((name, outs)))
                if out in self.outputs:
                    raise ValueError("More than one LLVM producer for " + out)
                self.outputs[out] = "gen/" + match[2]
                self.directories.update(parent.as_posix() for parent in Path(match[2]).parents)
        new_names = set()
        for relative, family in self.families.items():
            kinds = {row[1].func.id for row in family.values()}
            if len(kinds) != 1:
                raise ValueError("LLVM producer rule kinds disagree: " + relative)
            kind = next(iter(kinds))
            names = [text_constant(attributes(row[1])["name"]) for row in family.values()]
            if any(name is None for name in names):
                raise ValueError("LLVM producer name is not literal: " + relative)
            if len(names) == 1 and not any(cpu in names[0] for cpu in CPUS):
                name = names[0]
            else:
                prefix = "tablegen_" if kind == "genrule" else "configure_"
                name = prefix + re.sub(r"[^a-zA-Z0-9_]+", "_", relative).strip("_")
            if name in new_names or (name in existing_names and name not in names):
                raise ValueError("LLVM configured rule name already exists: " + name)
            new_names.add(name)
            for original in names:
                self.rename_rules[original] = name
            self.counts[(kind, "paired" if len(family) == 2 else "only_" + next(iter(family)))] += 1

    def value(self, value, raw=False):
        def generated(match):
            relative = (match[2] or "").rstrip("/")
            if ("gen/" + match[1] + "/" + relative in self.outputs
                    or relative in self.directories or relative == ""):
                return "gen" + ("/" + match[2] if match[2] else "")
            return match[0]

        if raw:
            result = GENERATED.sub(generated, value)
            if result.startswith(":") and result[1:] in self.rename_rules:
                return ":" + self.rename_rules[result[1:]]
            return result
        label = "@" + RAW + "//:"
        if value.startswith(label):
            tail = value.removeprefix(label)
            if tail in self.rename_rules:
                return label + self.rename_rules[tail]
            return label + GENERATED.sub(generated, tail)
        # any2bazel stores execution paths, never labels, for source_map.
        if value.startswith(ACTUAL_ROOT):
            return ACTUAL_ROOT + GENERATED.sub(generated, value.removeprefix(ACTUAL_ROOT))
        return value

    def node_edits(self, source, node, raw):
        edits = []

        def visit(current, is_raw):
            if (not is_raw and isinstance(current, ast.BinOp)
                    and any(raw_repository_call(child) for child in ast.walk(current))):
                is_raw = True
            value = text_constant(current)
            if value is not None:
                new = self.value(value, is_raw)
                if new != value:
                    edits.append((*source.span(current), json.dumps(new, ensure_ascii=False)))
            else:
                for child in ast.iter_child_nodes(current):
                    visit(child, is_raw)
        visit(node, raw)
        return edits

    def node_text(self, source, node, raw):
        start, end = source.span(node)
        chunks = []
        offset = start
        for lower, upper, replacement in sorted(self.node_edits(source, node, raw)):
            chunks.extend((source.bytes[offset:lower], replacement.encode()))
            offset = upper
        chunks.append(source.bytes[offset:end])
        return b"".join(chunks).decode()

    def producer(self, source, relative, family):
        cpus = [cpu for cpu in CPUS if cpu in family]
        attrs = {cpu: attributes(family[cpu][1]) for cpu in cpus}
        if any(set(attrs[cpu]) != set(attrs[cpus[0]]) for cpu in cpus):
            raise ValueError("LLVM genrule attributes disagree: " + relative)
        name = self.rename_rules[text_constant(attrs[cpus[0]]["name"])]
        outs = [self.outputs[out] for out in family[cpus[0]][2]]
        fields = []
        for field in attrs[cpus[0]]:
            if field == "name":
                value = json.dumps(name)
            elif field == "out":
                value = json.dumps(outs[0])
            elif field == "outs":
                value = json.dumps(outs)
            else:
                options = {cpu: self.node_text(source, attrs[cpu][field], True) for cpu in cpus}
                keys = {key_for_cpu(attrs[cpu][field], cpu, MapLLVM(self, raw=True)) for cpu in cpus}
                if len(keys) == 1:
                    value = options[cpus[0]]
                else:
                    if field not in CONFIGURABLE:
                        raise ValueError("LLVM cannot configure " + str((relative, field)))
                    value = ("select({\n" + "".join(
                        "        " + json.dumps(SETTINGS[cpu]) + ": " + options[cpu] + ",\n"
                        for cpu in cpus) + "    })")
                    self.counts[("select", field)] += 1
            fields.append("    " + field + " = " + value + ",\n")
        if len(cpus) == 1:
            if "target_compatible_with" in attrs[cpus[0]]:
                raise ValueError("LLVM single-CPU rule already had target compatibility: " + name)
            fields.append("    target_compatible_with = [" + json.dumps(SETTINGS[cpus[0]]) + "],\n")
        rendered = family[cpus[0]][1].func.id + "(\n" + "".join(fields) + ")"
        produced = attributes(ast.parse(rendered).body[0].value)
        for cpu in cpus:
            for field, original in attrs[cpu].items():
                key = key_for_cpu(original, cpu, MapLLVM(self, raw=True))
                if field == "name":
                    key = ast_key(ast.Constant(name))
                if key_for_cpu(produced[field], cpu) != key:
                    raise ValueError("LLVM generated action changed: " + str((relative, cpu, field)))
        return rendered

    def starlark(self, path, source):
        raw = path == self.path
        edits, producers = [], []
        if raw:
            for relative, family in self.families.items():
                rendered = self.producer(source, relative, family)
                for i, cpu in enumerate(cpu for cpu in CPUS if cpu in family):
                    start, end = source.span(family[cpu][0])
                    producers.append((start, end))
                    edits.append((start, end, rendered if i == 0 else ""))
        for lower, upper, replacement in self.node_edits(source, source.module, raw):
            if not any(start <= lower < end for start, end in producers):
                edits.append((lower, upper, replacement))
        if not edits:
            return None
        result = collapse_equal_cpu_selects(source.rewrite(edits))
        target = Source(result)
        old = {}
        for _, call in source.rules():
            name = text_constant(attributes(call)["name"])
            if not raw or name not in self.rename_rules:
                old[name] = call
        new_names = set(self.rename_rules.values())
        new = {}
        for _, call in target.rules():
            name = text_constant(attributes(call)["name"])
            if not raw or name not in new_names:
                new[name] = call
        if set(old) != set(new):
            raise ValueError("LLVM input or compiled target set changed: " + str(path))
        for name, call in old.items():
            for cpu in CPUS:
                if key_for_cpu(call, cpu, MapLLVM(self, raw)) != key_for_cpu(new[name], cpu):
                    raise ValueError("LLVM compiled rule changed: " + str((path, name, cpu)))
            self.counts[("proof", "unchanged_rules")] += 1
        self.verify_single_cpu_inputs(path, target, new)
        self.counts[("constant", "edits")] += len(edits) - len(producers)
        return result

    def verify_single_cpu_inputs(self, path, target, compiled):
        definitions = {
            statement.targets[0].id: statement.value
            for statement in target.module.body
            if (isinstance(statement, ast.Assign) and len(statement.targets) == 1
                and isinstance(statement.targets[0], ast.Name))
        }
        single = {
            "gen/" + relative: next(iter(family))
            for relative, family in self.families.items() if len(family) == 1
        }
        # Target-specific TableGen has multi-output rules. All outputs belong to
        # the same target CPU, not just each rule's first output.
        for family in self.families.values():
            if len(family) == 1:
                cpu = next(iter(family))
                single.update((self.outputs[old], cpu) for old in family[cpu][2])
        for name, call in compiled.items():
            if not isinstance(call.func, ast.Name) or call.func.id not in {
                    "cc_library", "ch_cc_library", "llvm_runtime_objects", "filegroup", "nasm_objects"}:
                continue
            for field, node in attributes(call).items():
                if field not in {"srcs", "hdrs", "textual_hdrs", "additional_compiler_inputs", "data"}:
                    continue
                expanded = BoundConstants(definitions).visit(copy.deepcopy(node))
                for cpu in CPUS:
                    active = ChooseCpu(cpu).visit(copy.deepcopy(expanded))
                    for child in ast.walk(active):
                        value = text_constant(child)
                        if value is None:
                            continue
                        if path == self.path and value.startswith(":"):
                            value = value[1:]
                        elif value.startswith("@" + RAW + "//:"):
                            value = value.removeprefix("@" + RAW + "//:")
                        else:
                            continue
                        if value in single and single[value] != cpu:
                            raise ValueError("Wrong target CPU reaches LLVM input: " + str((path, name, cpu, field, value)))
                        if value in single:
                            self.counts[("proof", "single_cpu_inputs")] += 1

    def metadata(self):
        path = self.root / "bazel/initial_migration/any2bazel.json"
        data = json.loads(path.read_text())
        for field in ("source_map", "include_map"):
            result = {}
            for row in data["ignore"][field]:
                original, identity = row["from"], row["to"]
                updated = self.value(original)
                if updated != original:
                    self.counts[("metadata", field)] += 1
                    if field == "include_map":
                        for cpu in CPUS:
                            identity = identity.replace("@build/" + cpu + "/", "@build/")
                if updated in result and result[updated] != identity:
                    raise ValueError("Conflicting LLVM any2bazel identity: " + str((field, updated, result[updated], identity)))
                result[updated] = identity
            data["ignore"][field] = [{"from": path, "to": value} for path, value in result.items()]
        return path, json.dumps(data, indent=2) + "\n"

    def run(self, write=False, *, read_only=()):
        self.collect(Source(self.path.read_text()))
        if not self.families:
            print("No duplicated LLVM gen/<cpu> output paths.")
            return
        changed = {}
        tracked = subprocess.check_output(
            ["git", "ls-files", "-z", "--", "*.bazel", "*.bzl"], cwd=self.root,
        ).decode().split("\0")
        for relative in tracked:
            if not relative:
                continue
            path = self.root / relative
            if not path.is_file():
                continue
            text = path.read_text()
            if not GENERATED.search(text) and RAW not in text:
                continue
            result = self.starlark(path, Source(text))
            if result is not None:
                changed[path] = result
        path, result = self.metadata()
        if path.read_text() != result:
            changed[path] = result
        if set(changed).intersection(read_only):
            raise ValueError('Update LLVM output labels in the maintained BUILD file before importing')
        if write:
            for path, text in changed.items():
                path.write_text(text)
        print(json.dumps({
            "write": write,
            "producer_rules_before": sum(len(family) for family in self.families.values()),
            "producer_rules_after": len(self.families),
            "generated_files_before": len(self.outputs),
            "generated_files_after": len(set(self.outputs.values())),
            "counts": {"/".join(key): value for key, value in sorted(self.counts.items())},
            "files": [str(path.relative_to(self.root)) for path in sorted(changed)],
        }, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    Migration(args.root.resolve()).run(args.write)


if __name__ == "__main__":
    main()

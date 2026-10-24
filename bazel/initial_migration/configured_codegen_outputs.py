#!/usr/bin/env python3
"""Instantiate identical third-party code generation once per Bazel configuration.

The CMake migrations stored identical genrules twice through codegen_for_arch.
Their output labels can be shared because Bazel selects the bin directory.
Unpaired outputs, including the x86 HDFS crc object, remain unchanged.
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
    CPUS, MapConstants, attributes, collapse_equal_cpu_selects, key_for_cpu, text_constant,
)

ROOT = Path(__file__).resolve().parents[2]
GENERATED = re.compile(r"gen/(?:x86_64|aarch64|\{arch\})/(contrib/[A-Za-z0-9_+./{}-]+)")
SHARED = "_SHARED_CODEGEN"


def data_assignments(source):
    return {
        statement.targets[0].id: statement.value
        for statement in source.module.body
        if (isinstance(statement, ast.Assign) and len(statement.targets) == 1
            and isinstance(statement.targets[0], ast.Name))
    }


def dictionary(node):
    if not isinstance(node, ast.Dict):
        raise ValueError("Expected dictionary, got " + ast.dump(node))
    result = {}
    for key, value in zip(node.keys, node.values, strict=True):
        name = text_constant(key)
        if name is None or name in result:
            raise ValueError("Invalid dictionary key " + ast.dump(key))
        result[name] = value
    return result


class Instantiate(ast.NodeTransformer):
    """Reproduce codegen_for_arch on literal parts of each original attribute."""

    def __init__(self, cpu):
        self.cpu = cpu

    def visit_Constant(self, node):
        if isinstance(node.value, str):
            return ast.copy_location(
                ast.Constant(node.value.replace("gen/{arch}/", "gen/" + self.cpu + "/")), node)
        return node


class Migration:
    def __init__(self, root):
        self.root = root
        self.path = root / "bazel/rules/codegen_rules.bzl"
        self.pairs = defaultdict(dict)
        self.output_paths = set()
        self.output_directories = set()
        self.rename_rules = {}
        self.counts = Counter()

    def collect(self, source):
        declarations = data_assignments(source)
        self.templates = dictionary(declarations[SHARED])
        self.original = dictionary(declarations["CODEGEN_RULES"])
        for name, value in self.original.items():
            if not (isinstance(value, ast.Call) and isinstance(value.func, ast.Name)
                    and value.func.id == "codegen_for_arch" and len(value.args) == 2):
                continue
            ref = value.args[0]
            cpu = text_constant(value.args[1])
            if not (isinstance(ref, ast.Subscript) and isinstance(ref.value, ast.Name)
                    and ref.value.id == SHARED and cpu in CPUS):
                raise ValueError("Unrecognized codegen_for_arch: " + source.text(value))
            name_ref = text_constant(ref.slice)
            if name_ref not in self.templates:
                raise ValueError("Missing shared codegen template: " + repr(name_ref))
            # First-party grpc and #embed files keep their present labels.
            if not name_ref.startswith("contrib/"):
                continue
            if cpu in self.pairs[name_ref]:
                raise ValueError("Duplicate codegen_for_arch: " + str((name_ref, cpu)))
            self.pairs[name_ref][cpu] = name

        for output, pair in self.pairs.items():
            if set(pair) != set(CPUS):
                raise ValueError("Unpaired third-party codegen: " + str((output, pair)))
            rule = dictionary(self.templates[output])
            outs = rule.get("outs")
            if not isinstance(outs, ast.List):
                raise ValueError("Output list is not literal: " + output)
            for node in outs.elts:
                old = text_constant(node)
                if not old or not old.startswith("gen/{arch}/contrib/"):
                    raise ValueError("Shared codegen also produces other files: " + str((output, old)))
                name = old.removeprefix("gen/{arch}/")
                if name in self.output_paths:
                    raise ValueError("Two genrules produce " + name)
                self.output_paths.add(name)
                self.output_directories.update(parent.as_posix() for parent in Path(name).parents)
            name = "generate_" + re.sub(r"[^a-zA-Z0-9_]+", "_", output)
            if name in self.original or name in self.rename_rules.values():
                raise ValueError("Colliding genrule name: " + name)
            for original in pair.values():
                self.rename_rules[original] = name

    def value(self, value, owner):
        # Do not rewrite every name below a migrated directory: HDFS also has
        # a gen/x86_64 crc object that belongs to an unrelated unpaired action.
        def generated(match):
            suffix = match[1].rstrip("/")
            if suffix in self.output_paths or suffix in self.output_directories:
                return "gen/" + match[1]
            return match[0]

        result = GENERATED.sub(generated, value)
        for prefix in ("//bazel:", "@clickhouse//bazel:"):
            if result.startswith(prefix) and result[len(prefix):] in self.rename_rules:
                return prefix + self.rename_rules[result[len(prefix):]]
        if owner == "bazel" and result.startswith(":") and result[1:] in self.rename_rules:
            return ":" + self.rename_rules[result[1:]]
        return result

    def translated_text(self, source, node, owner=None):
        start, end = source.span(node)
        edits = []
        for child in ast.walk(node):
            old = text_constant(child)
            if old is not None:
                new = self.value(old, owner)
                if new != old:
                    edits.append((*source.span(child), json.dumps(new, ensure_ascii=False)))
        offset = start
        chunks = []
        for lower, upper, text in sorted(edits):
            chunks.extend((source.bytes[offset:lower], text.encode()))
            offset = upper
        chunks.append(source.bytes[offset:end])
        return b"".join(chunks).decode()

    def codegen(self, source):
        declarations = data_assignments(source)
        template_fields, codegen_fields = [], []
        for name, node in self.templates.items():
            if name not in self.pairs:
                template_fields.append(json.dumps(name) + ": " + source.text(node))
        emitted = set()
        for name, node in self.original.items():
            if name not in self.rename_rules:
                codegen_fields.append(json.dumps(name) + ": " + self.translated_text(source, node))
                continue
            shared_name = self.rename_rules[name]
            if shared_name in emitted:
                continue
            emitted.add(shared_name)
            output = next(output for output, pair in self.pairs.items() if name in pair.values())
            rendered = self.translated_text(source, self.templates[output])
            parsed = ast.parse(rendered, mode="eval").body
            for cpu in CPUS:
                original = Instantiate(cpu).visit(copy.deepcopy(self.templates[output]))
                if key_for_cpu(original, cpu, MapConstants(self, None)) != key_for_cpu(parsed, cpu):
                    raise ValueError("Generated action changed: " + str((output, cpu)))
            codegen_fields.append(json.dumps(shared_name) + ": " + rendered)
            self.counts["identical_action_pairs"] += 1
        replacements = [
            (*source.span(declarations[SHARED]), "{\n" + ",\n".join(template_fields) + ",\n}"),
            (*source.span(declarations["CODEGEN_RULES"]), "{\n" + ",\n".join(codegen_fields) + ",\n}"),
        ]
        result = source.rewrite(replacements)
        updated = data_assignments(Source(result))
        rules = dictionary(updated["CODEGEN_RULES"])
        for name, node in self.original.items():
            if name in self.rename_rules:
                continue
            for cpu in CPUS:
                if key_for_cpu(node, cpu, MapConstants(self, None)) != key_for_cpu(rules[name], cpu):
                    raise ValueError("Unrelated generator changed: " + name)
            self.counts["other_generators_preserved"] += 1
        if set(rules) != (set(self.original) - set(self.rename_rules)) | set(self.rename_rules.values()):
            raise ValueError("Unexpected CODEGEN_RULES names after migration")
        return result

    def starlark(self, path, source):
        owner = "bazel" if path == self.root / "bazel/BUILD.bazel" else None
        edits = []
        for node in ast.walk(source.module):
            value = text_constant(node)
            if value is None:
                continue
            new = self.value(value, owner)
            if new != value:
                edits.append((*source.span(node), json.dumps(new, ensure_ascii=False)))
        if not edits:
            return None
        new = collapse_equal_cpu_selects(source.rewrite(edits))
        updated = Source(new)
        before = {text_constant(attributes(call)["name"]): call for _, call in source.rules()}
        after = {text_constant(attributes(call)["name"]): call for _, call in updated.rules()}
        if set(before) != set(after):
            raise ValueError("Compiled rule set changed in " + str(path))
        for name, call in before.items():
            for cpu in CPUS:
                if key_for_cpu(call, cpu, MapConstants(self, owner)) != key_for_cpu(after[name], cpu):
                    raise ValueError("Compiled rule order or content changed in " + str((path, name, cpu)))
            self.counts["unchanged_compiled_rules"] += 1
        self.counts["consumer_constant_edits"] += len(edits)
        return new

    def metadata(self):
        path = self.root / "bazel/initial_migration/any2bazel.json"
        data = json.loads(path.read_text())
        identities = {
            "@build/" + cpu + "/" + output: "@build/" + output
            for cpu in CPUS for output in self.output_paths
        }
        result = {}
        for row in data["ignore"]["source_map"]:
            original, identity = row["from"], row["to"]
            if identity in identities:
                original = self.value(original, None)
                identity = identities[identity]
                self.counts["mapped_source_identities"] += 1
            if original in result and result[original] != identity:
                raise ValueError("Conflicting any2bazel source identity: " + str((original, result[original], identity)))
            result[original] = identity
        data["ignore"]["source_map"] = [{"from": key, "to": value} for key, value in result.items()]
        includes = {row["from"]: row["to"] for row in data["ignore"]["include_map"]}
        source = "bazel-out/k8-opt/bin/bazel/gen/contrib"
        identity = "@build/contrib"
        if source in includes and includes[source] != identity:
            raise ValueError("Conflicting any2bazel include identity: " + source)
        includes[source] = identity
        data["ignore"]["include_map"] = [{"from": key, "to": value} for key, value in includes.items()]
        return path, json.dumps(data, indent=2) + "\n"

    def generated_aggregate(self, changed):
        path = self.root / "bazel/BUILD.bazel"
        source = changed.get(path, path.read_text())
        old = 'if name.startswith("generate_" + arch + "_")'
        new = old + ' or name.startswith("generate_contrib_")'
        if new in source:
            return path, source
        if source.count(old) != 1:
            raise ValueError("Cannot identify //bazel:generated_<arch> filegroups")
        result = source.replace(old, new)
        ast.parse(result)
        return path, result

    def run(self, write=False, read_only=()):
        source = Source(self.path.read_text())
        self.collect(source)
        if not self.pairs:
            print("No duplicated external codegen rules.")
            return
        changed = {self.path: self.codegen(source)}
        tracked = subprocess.check_output(
            ["git", "ls-files", "-z", "--", "*.bazel", "*.bzl"], cwd=self.root,
        ).decode().split("\0")
        for relative in tracked:
            if not relative:
                continue
            path = self.root / relative
            if path == self.path or path in read_only or not path.is_file():
                continue
            text = path.read_text()
            if not GENERATED.search(text) and not any(name in text for name in self.rename_rules):
                continue
            result = self.starlark(path, Source(text))
            if result is not None:
                changed[path] = result
        path, text = self.metadata()
        changed[path] = text
        path, text = self.generated_aggregate(changed)
        changed[path] = text
        if write:
            for path, text in changed.items():
                path.write_text(text)
        print(json.dumps({
            "write": write,
            "shared_rules": len(self.pairs),
            "shared_outputs": len(self.output_paths),
            "counts": dict(sorted(self.counts.items())),
            "files": [str(path.relative_to(self.root)) for path in sorted(changed)],
        }, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--root", type=Path, default=ROOT)
    options = parser.parse_args()
    Migration(options.root.resolve()).run(write=options.write)


if __name__ == "__main__":
    main()

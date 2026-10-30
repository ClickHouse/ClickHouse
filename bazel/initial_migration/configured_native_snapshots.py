#!/usr/bin/env python3
"""Publish the captured native CMake configuration through configured outputs."""

import argparse
import ast
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bazel"))
from initial_migration.compact_build import Source, ast_key
from initial_migration.configured_external_outputs import (
    CPUS, MapConstants, attributes, collapse_equal_cpu_selects, key_for_cpu,
    text_constant,
)

SNAPSHOTS = {
    "base/base/GitHash.generated.cpp": "configure_git_hash",
    "includes/configs/config.h": "configure_clickhouse_config",
    "includes/configs/config_tools.h": "configure_clickhouse_tools",
    "includes/configs/config_version.cpp": "configure_clickhouse_version",
}
LABEL = re.compile(r"(@clickhouse)?//bazel:config/(x86_64|aarch64)/([^\s)\"']+)")
INCLUDE = re.compile(r"bazel/config/(x86_64|aarch64)/(includes/configs|base|\.)(?=/|$)")
NATIVE_DIRS = ("../../bazel/gen/includes/configs", "..", "../../bazel/gen/base")
BASE_RAW_DIRS = ("$(GENDIR)/bazel/gen/includes/configs", "base", "$(GENDIR)/bazel/gen/base")


def digest(value):
    return hashlib.sha256(value).hexdigest()


class Migration:
    def __init__(self, root, ref=None):
        self.root = root
        self.ref = ref
        self.counts = Counter()
        self.before = {}

    def read(self, path):
        relative = str(path.relative_to(self.root))
        if self.ref:
            value = subprocess.check_output(["git", "show", self.ref + ":" + relative], cwd=self.root)
        else:
            value = path.read_bytes()
        self.before[path] = value
        return value.decode()

    def value(self, value, field):
        value = LABEL.sub(
            lambda match: (match[1] or "") + "//bazel:gen/" + match[3]
            if match[3] in SNAPSHOTS else match[0], value)
        if field in ("copts", "conlyopts", "cxxopts", "local_includes", "includes"):
            def include(match):
                directory = "" if match[2] == "." else "/" + match[2]
                prefix = "$(GENDIR)/" if field in ("copts", "conlyopts", "cxxopts") else ""
                return prefix + "bazel/gen" + directory
            value = INCLUDE.sub(include, value)
        return value

    @staticmethod
    def declarations(source):
        return {
            stmt.targets[0].id: stmt.value
            for stmt in source.module.body
            if (isinstance(stmt, ast.Assign) and len(stmt.targets) == 1
                and isinstance(stmt.targets[0], ast.Name))
        }

    def field_context(self, source):
        constants = self.declarations(source)
        contexts = defaultdict(set)
        def visit(node, field, expanding):
            contexts[id(node)].add(field)
            if isinstance(node, ast.Name) and node.id in constants and node.id not in expanding:
                visit(constants[node.id], field, expanding | {node.id})
            for child in ast.iter_child_nodes(node):
                visit(child, field, expanding)
        for _, call in source.rules():
            for field, value in attributes(call).items():
                visit(value, field, set())
        return contexts

    def native_build(self, path, original):
        source = Source(original)
        contexts = self.field_context(source)
        edits = []
        for node in ast.walk(source.module):
            old = text_constant(node)
            if old is None:
                continue
            # File label references are independent of the field. Include
            # strings need a bin-directory prefix only in compiler options.
            candidates = {self.value(old, field) for field in contexts.get(id(node), {None})}
            if len(candidates) != 1:
                raise ValueError(f"{path}: one variable used as compiler option and include attribute: {old}")
            new = next(iter(candidates))
            if new != old:
                edits.append((*source.span(node), json.dumps(new, ensure_ascii=False)))
        if not edits:
            return original
        result = collapse_equal_cpu_selects(source.rewrite(edits))
        after = Source(result)
        before_names = {text_constant(attributes(call)["name"]): call for _, call in source.rules()}
        after_names = {text_constant(attributes(call)["name"]): call for _, call in after.rules()}
        if set(before_names) != set(after_names):
            raise ValueError("Rule names changed in " + str(path))
        for name, call in before_names.items():
            a, b = attributes(call), attributes(after_names[name])
            if set(a) != set(b):
                raise ValueError(f"{path}:{name} attribute names changed")
            for field, old in a.items():
                for cpu in CPUS:
                    if key_for_cpu(old, cpu, MapConstants(self, field)) != key_for_cpu(b[field], cpu):
                        raise ValueError(f"{path}:{name} {cpu} changed {field} beyond configured input mapping")
        oldconst, newconst = self.declarations(source), self.declarations(after)
        if set(oldconst) != set(newconst):
            raise ValueError("Assigned variable names changed in " + str(path))
        for name, node in oldconst.items():
            fields = contexts.get(id(node), {None})
            for field in fields:
                for cpu in CPUS:
                    if key_for_cpu(node, cpu, MapConstants(self, field)) != key_for_cpu(newconst[name], cpu):
                        raise ValueError(f"{path} {name} {cpu} changed beyond configured input mapping")
        self.counts["preserved_rules"] += len(before_names)
        self.counts["mapped_constants"] += len(edits)
        return result

    @staticmethod
    def concat_parts(node):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return Migration.concat_parts(node.left) + Migration.concat_parts(node.right)
        if isinstance(node, ast.List):
            return [(True, item) for item in node.elts]
        return [(False, node)]

    @staticmethod
    def join_parts(source, parts):
        chunks, pending = [], []
        for element, node in parts:
            if element:
                pending.append(source.text(node))
            else:
                if pending:
                    chunks.append("[" + ", ".join(pending) + "]")
                    pending = []
                chunks.append("(" + source.text(node) + ")")
        if pending:
            chunks.append("[" + ", ".join(pending) + "]")
        return " + ".join(chunks) if chunks else "[]"

    def base_includes(self, content):
        source = Source(content)
        rules = {text_constant(attributes(call)["name"]): call for _, call in source.rules()}
        attrs = attributes(rules["common"])
        copts = self.concat_parts(attrs["copts"])
        removed, kept = [], []
        index = 0
        while index < len(copts):
            if (index + 1 < len(copts) and copts[index][0] and copts[index + 1][0]
                    and text_constant(copts[index][1]) == "-I"
                    and text_constant(copts[index + 1][1]) in BASE_RAW_DIRS):
                removed.append(text_constant(copts[index + 1][1]))
                index += 2
            else:
                kept.append(copts[index])
                index += 1
        if not removed:
            # A generator rerun may retain an already-hand-edited BUILD.
            return content
        if tuple(removed) != BASE_RAW_DIRS:
            raise ValueError("//base/base:common changed three source/config -I inputs: " + repr(removed))
        result = source.rewrite([
            (*source.span(attrs["copts"]), self.join_parts(source, kept)),
            (*source.span(attrs["local_includes"]),
             json.dumps(NATIVE_DIRS) + " + (" + source.text(attrs["local_includes"]) + ")"),
        ])
        checked = Source(result)
        new = attributes({text_constant(attributes(c)["name"]): c for _, c in checked.rules()}["common"])
        for cpu in CPUS:
            a = key_for_cpu(ast.parse(self.join_parts(source, kept), mode="eval").body, cpu)
            b = key_for_cpu(new["copts"], cpu)
            if a != b:
                raise ValueError("Other common flags changed")
            expected = json.dumps(NATIVE_DIRS) + " + (" + source.text(attrs["local_includes"]) + ")"
            if key_for_cpu(ast.parse(expected, mode="eval").body, cpu) != key_for_cpu(new["local_includes"], cpu):
                raise ValueError("Other common local_includes changed")
        self.counts["moved_common_include_pairs"] += len(removed)
        return result

    def declare_copies(self, text):
        source = Source(text)
        named = {text_constant(attributes(c)["name"]): c for _, c in source.rules()}
        present = set(named) & set(SNAPSHOTS.values())
        if present and present != set(SNAPSHOTS.values()):
            raise ValueError("Incomplete native configured snapshot declarations")
        if not present:
            statement, = [s for s in source.module.body if isinstance(s, ast.Expr)
                          and isinstance(s.value, ast.Call)
                          and getattr(s.value.func, "id", "") == "package"]
            after_package = source.span(statement)[1]
            copies = []
            for output, name in SNAPSHOTS.items():
                choices = {"@platforms//cpu:" + cpu: "config/" + cpu + "/" + output for cpu in CPUS}
                copies.append(
                    "copy_file(\n"
                    f"    name = {json.dumps(name)},\n"
                    f"    src = select({json.dumps(choices)}),\n"
                    f"    out = {json.dumps('gen/' + output)},\n"
                    "    allow_symlink = False,\n"
                    ")\n")
            text = text[:after_package] + "\n\n" + "\n".join(copies) + text[after_package:]
            load = 'load("@bazel_skylib//rules:copy_file.bzl", "copy_file")\n'
            if load not in text:
                text = load + text
        for node in ast.walk(Source(text).module):
            if not isinstance(node, ast.Call):
                continue
            attrs = attributes(node)
            name = text_constant(attrs.get("name"))
            if name not in SNAPSHOTS.values():
                continue
            out = text_constant(attrs["out"])
            if out != "gen/" + next(output for output, wanted in SNAPSHOTS.items() if wanted == name):
                raise ValueError("copy_file renamed output: " + str(name))
            for cpu in CPUS:
                chosen = key_for_cpu(attrs["src"], cpu)
                expected = ast_key(ast.Constant("config/" + cpu + "/" + out.removeprefix("gen/")))
                if chosen != expected:
                    raise ValueError(f"copy_file {name} selects the wrong {cpu} bytes")
        return text

    def prefix_maps(self, text):
        archmap = '"-ffile-prefix-map=$(GENDIR)/bazel/gen/" + arch + "/=" + build,'
        base = '"-ffile-prefix-map=$(GENDIR)/bazel/gen/base/=" + build + "base/",'
        includes = '"-ffile-prefix-map=$(GENDIR)/bazel/gen/includes/=" + build + "includes/",'
        if base not in text:
            if text.count(archmap) != 1:
                raise ValueError("Cannot find saved source prefix map in repository_paths.bzl")
            text = text.replace(archmap, archmap + "\n        " + base + "\n        " + includes)
        return text

    def parity(self, text):
        data = json.loads(text)
        mappings = {}
        cfg_sources = {"bazel/config/" + cpu + "/" + output: "bazel-out/k8-opt/bin/bazel/gen/" + output
                       for cpu in CPUS for output in SNAPSHOTS}
        identities = {"@build/" + cpu + "/" + output: "@build/" + output
                      for cpu in CPUS for output in SNAPSHOTS}
        for row in data["ignore"]["source_map"]:
            source, dest = cfg_sources.get(row["from"], row["from"]), identities.get(row["to"], row["to"])
            if source in mappings and mappings[source] != dest:
                raise ValueError("Conflicting source parity " + str((source, mappings[source], dest)))
            mappings[source] = dest
        data["ignore"]["source_map"] = [{"from": source, "to": dest} for source, dest in mappings.items()]
        include_map = {r["from"]: r["to"] for r in data["ignore"]["include_map"]}
        for directory in ("base", "includes"):
            include_map["bazel-out/k8-opt/bin/bazel/gen/" + directory] = "@build/" + directory
        data["ignore"]["include_map"] = [{"from": source, "to": dest} for source, dest in include_map.items()]
        return json.dumps(data, indent=2) + "\n"

    def prepare(self, generated_builds=()):
        tracked = subprocess.check_output([
            "git", "ls-files", "-z", "--", "*.bazel", "*.bzl",
        ], cwd=self.root).decode().split("\0")
        tracked = list(dict.fromkeys(tracked + [
            path.relative_to(self.root).as_posix() for path in generated_builds
        ]))
        changes = {}
        for relative in tracked:
            if not relative:
                continue
            path = self.root / relative
            if (path.name != "BUILD.bazel" and not path.name.endswith(".BUILD.bazel")) or relative == "bazel/BUILD.bazel":
                continue
            if self.ref is None and not path.exists():
                continue
            original = self.read(path)
            if "config/x86_64/" not in original and "config/aarch64/" not in original:
                continue
            content = self.native_build(path, original)
            if relative == "base/base/BUILD.bazel":
                content = self.base_includes(content)
            if content != original:
                changes[path] = content
        for path, update in (
                (self.root / "bazel/BUILD.bazel", self.declare_copies),
                (self.root / "bazel/rules/repository_paths.bzl", self.prefix_maps),
                (self.root / "bazel/initial_migration/any2bazel.json", self.parity)):
            original = self.read(path)
            content = update(original)
            if content != original:
                changes[path] = content
        return changes

    def report(self, changes):
        snapshot_hashes = {}
        for name in SNAPSHOTS:
            snapshot_hashes[name] = {}
            for cpu in CPUS:
                p = self.root / "bazel/config" / cpu / name
                snapshot_hashes[name][cpu] = digest(self.read(p).encode())
        return {
            "ref": self.ref,
            "counts": dict(self.counts),
            "snapshot_sha256": snapshot_hashes,
            "files": {str(p.relative_to(self.root)): {
                "old": digest(self.before[p]), "new": digest(content.encode()),
            } for p, content in changes.items()},
        }

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--ref", help="Read files at a frozen commit for a read-only preview.")
    parser.add_argument("--preview", type=Path, help="Write a patch and JSON proof under this directory.")
    parser.add_argument("--write", action="store_true")
    options = parser.parse_args()
    if options.ref and options.write:
        parser.error("--ref cannot be combined with --write")
    migration = Migration(options.root.resolve(), options.ref)
    changes = migration.prepare()
    report = migration.report(changes)
    if options.preview:
        import difflib
        options.preview.mkdir(parents=True, exist_ok=True)
        patch = []
        for p, content in changes.items():
            filename = str(p.relative_to(migration.root))
            patch.extend(difflib.unified_diff(migration.before[p].decode().splitlines(keepends=True),
                           content.splitlines(keepends=True), "a/" + filename, "b/" + filename))
        (options.preview / "snapshots.patch").write_text("".join(patch))
        (options.preview / "snapshots.json").write_text(json.dumps(report, indent=2) + "\n")
    if options.write:
        for path, content in changes.items():
            if path.read_bytes() != migration.before[path]:
                raise ValueError("Concurrent edits changed " + str(path))
        for path, content in changes.items():
            path.write_text(content)
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()

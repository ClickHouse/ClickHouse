#!/usr/bin/env python3
"""Preserve captured link order after an intentional external BUILD srcs change.

Use --before and --after for the commits containing the old and new BUILD
files, --facts for scan_include_adjacency.py v3 pinned repository inventories,
and --metadata-rev for the commit whose existing link plans are to be updated.
For example, run with --patch /tmp/link-order.patch first and --write after
review. This does not regenerate a CMake plan or run during a Bazel build.
"""

import argparse
import ast
from collections import Counter
import copy
from difflib import unified_diff
import json
from pathlib import Path
import posixpath
import subprocess
import sys

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bazel"))

from initial_migration.compact_build import Source
from initial_migration.compact_external_options import (
    CC_COMPILERS, CPUS, SOURCE_SUFFIXES, attrs, function, load_package_files,
)
from initial_migration.cmake_link_plan import object_ranges


def git(*arguments):
    return subprocess.check_output(["git", *arguments], cwd=ROOT, text=True)


def at(revision, filename):
    return git("show", revision + ":" + filename)


class Build:
    def __init__(self, text, files, arch, filename):
        self.doc = Source(text)
        self.files = files
        self.arch = arch
        self.filename = filename
        self.constants = {
            stmt.targets[0].id: stmt.value
            for stmt in self.doc.module.body
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1
            and isinstance(stmt.targets[0], ast.Name)
        }
        self.rules = {}
        for _, rule in self.doc.rules():
            fields = attrs(rule)
            name = fields.get("name")
            if function(rule) in CC_COMPILERS and isinstance(name, ast.Constant):
                self.rules[name.value] = fields

    def value(self, node, active=()):
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, (ast.List, ast.Tuple)):
            return [self.value(element, active) for element in node.elts]
        if isinstance(node, ast.Name) and node.id in self.constants and node.id not in active:
            return self.value(self.constants[node.id], (*active, node.id))
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return self.value(node.left, active) + self.value(node.right, active)
        if function(node) == "select" and len(node.args) == 1 and isinstance(node.args[0], ast.Dict):
            branches = node.args[0]
            choices = dict(zip((self.value(key, active) for key in branches.keys), branches.values))
            branch = choices.get(self.arch, choices.get("//conditions:default"))
            if branch is not None:
                return self.value(branch, active)
        if function(node) == "glob" and len(node.args) == 1:
            options = {kw.arg: self.value(kw.value, active) for kw in node.keywords}
            if (options.keys() - {"exclude", "allow_empty", "exclude_directories"}
                    or options.get("exclude_directories", 1) not in (True, 1)):
                raise ValueError(f"{self.filename}: unsupported glob options {options}")
            return list(self.files.result(self.value(node.args[0], active), options.get("exclude", [])))
        raise ValueError(f"{self.filename}: cannot resolve {self.arch} srcs: {ast.unparse(node)}")

    def sources(self, name):
        node = self.rules[name].get("srcs")
        if node is None:
            return []
        values = self.value(node)
        if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
            raise ValueError(f"{self.filename}:{name}: srcs is not a list of filenames")
        # Labels for generated .cc outputs retain repository and package names.
        # Remove only the optional colon on filenames in the same package.
        return [value.removeprefix(":") for value in values
                if posixpath.splitext(value)[1] in SOURCE_SUFFIXES]


def permutations(plan, before, after, facts):
    old_pins = json.loads(at(before, "bazel/rules/contrib_repositories.json"))
    new_pins = json.loads(at(after, "bazel/rules/contrib_repositories.json"))
    repos = {spec["name"]: (owner, spec) for owner, spec in old_pins.items()}
    files = git("diff", "--name-only", before, after, "--", "bazel/dependencies").splitlines()
    changed = {Path(name).name.removesuffix(".BUILD.bazel"): name
               for name in files if name.endswith(".BUILD.bazel")}
    documents = {}
    remaps = {}
    for index, label in enumerate(plan["refs"]):
        repository, separator, name = label.partition("//:")
        if not separator or not repository.startswith("@"):
            continue
        repository = repository.removeprefix("@")
        if repository not in changed or repository not in repos:
            continue
        if repository not in documents:
            owner, spec = repos[repository]
            if new_pins.get(owner) != spec:
                raise ValueError(f"{repository}: source pin changed; a source-order remap is unsafe")
            inventory = load_package_files(facts, owner, spec)
            if inventory is None:
                raise ValueError(f"{repository}: missing verified pinned source inventory")
            filename = changed[repository]
            documents[repository] = (
                Build(at(before, filename), inventory, plan["_cpu"], filename),
                Build(at(after, filename), inventory, plan["_cpu"], filename),
            )
        old, new = documents[repository]
        # A later metadata commit may have added a new reference (e.g. to a
        # cc_proto_library). Never edit references absent at both input commits.
        if name not in old.rules and name not in new.rules:
            continue
        if name not in old.rules or name not in new.rules:
            raise ValueError(f"{label}: rule was replaced; this tool only remaps existing compilation groups")
        original, current = old.sources(name), new.sources(name)
        if original == current:
            continue
        expected = plan["expected_objects"][index]
        if (len(original) != expected or len(current) != expected
                or len(set(original)) != expected or len(set(current)) != expected
                or set(original) != set(current)):
            raise ValueError(f"{label} {plan['_cpu']}: expected {expected} distinct unchanged TUs, "
                             f"old={len(original)} new={len(current)}, "
                             f"removed={set(original) - set(current)}, added={set(current) - set(original)}")
        positions = {source: slot for slot, source in enumerate(current)}
        permutation = [positions[source] for source in original]
        if sorted(permutation) != list(range(expected)):
            raise ValueError(f"{label}: mapping is not a permutation")
        if [current[slot] for slot in permutation] != original:
            raise ValueError(f"{label}: mapping changed the captured compilation order")
        remaps[index] = permutation
    return remaps


def remap_object(operand, remaps):
    kind, ref, index = operand
    return [kind, ref, remaps[ref][index] if ref in remaps else index]


def remap_ranges(ranges, remaps):
    if not any(ref in remaps for ref, _, _ in ranges):
        return ranges
    return object_ranges([remap_object(["object", ref, index], remaps)
                          for ref, start, end in ranges for index in range(start, end)])


def render(revision, path, arch, before, after, facts):
    text = at(revision, path)
    doc = Source(text)
    definition = next(stmt for stmt in doc.module.body
                      if isinstance(stmt, ast.Assign) and any(
                          isinstance(target, ast.Name) and target.id == "CMAKE_LINK"
                          for target in stmt.targets))
    fields = dict(zip((ast.literal_eval(key) for key in definition.value.keys), definition.value.values))
    plan = ast.literal_eval(definition.value)
    plan["_cpu"] = arch
    maps = permutations(plan, before, after, facts)
    counts = Counter(refs=len(maps), moved_objects=sum(
        sum(index != value for index, value in enumerate(p)) for p in maps.values()))
    edits = []
    intended = copy.deepcopy(plan)
    for position, (node, record) in enumerate(zip(fields["archives"].elts, plan["archives"], strict=True)):
        mapped = copy.deepcopy(record)
        if record.get("file") in maps:
            ref = mapped.pop("file")
            mapped["objects"] = remap_ranges([[ref, 0, plan["expected_objects"][ref]]], maps)
            counts["file_archives"] += 1
        for field in ("objects", "append"):
            if field in record:
                mapped[field] = remap_ranges(record[field], maps)
                if mapped[field] != record[field]:
                    counts[field + "_archives"] += 1
        if mapped != record:
            # Keep captured plans concise: each member range is three integers.
            indent = " " * node.col_offset
            lines = [indent + "  " + json.dumps(key) + ": " + json.dumps(value)
                     for key, value in mapped.items()]
            edits.append((*doc.span(node), "{\n" + ",\n".join(lines) + "\n" + indent + "}"))
            intended["archives"][position] = mapped
    for position, (node, operand) in enumerate(zip(fields["sequence"].elts, plan["sequence"], strict=True)):
        if operand[0] == "objects":
            ranges = operand[1]
            mapped = ["objects", remap_ranges(ranges, maps)]
            counts["direct_objects"] += sum(
                maps[ref][index] != index for ref, start, end in ranges if ref in maps
                for index in range(start, end))
        elif operand[0] == "object":  # Older --metadata-rev plans store one object per entry.
            mapped = remap_object(operand, maps)
            counts["direct_objects"] += mapped != operand
        else:
            continue
        if mapped != operand:
            edits.append((*doc.span(node), json.dumps(mapped)))
            intended["sequence"][position] = mapped
    updated = doc.rewrite(edits)
    newdef = next(stmt for stmt in ast.parse(updated).body
                  if isinstance(stmt, ast.Assign) and any(
                      isinstance(target, ast.Name) and target.id == "CMAKE_LINK"
                      for target in stmt.targets))
    del intended["_cpu"]
    if ast.literal_eval(newdef.value) != intended:
        raise ValueError(f"{path}: rendered metadata differs from the verified remap")
    counts["archive_records"] = len(plan["archives"])
    return text, updated, dict(counts), {plan["refs"][ref]: value for ref, value in maps.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True, help="Commit before intentional BUILD source reordering")
    parser.add_argument("--after", required=True, help="Commit after intentional BUILD source reordering")
    parser.add_argument("--metadata-rev", default="HEAD", help="Read the original plan at this commit")
    parser.add_argument("--facts", type=Path, required=True, help="Pinned scan_include_adjacency.py v3 inventories")
    parser.add_argument("--patch", type=Path, help="Write an independently applicable link-metadata patch")
    parser.add_argument("--write", action="store_true", help="Write verified plans; requires pristine metadata-rev")
    options = parser.parse_args()
    paths = (
        ("bazel/rules/cmake_link_data.bzl", CPUS[0]),
        ("bazel/rules/cmake_link_data_aarch64.bzl", CPUS[1]),
    )
    outputs, patch, reports = {}, [], {}
    for path, arch in paths:
        original, updated, counts, maps = render(
            options.metadata_rev, path, arch, options.before, options.after, options.facts)
        if options.write and (ROOT / path).read_text() != original:
            raise ValueError(f"{path} changed since {options.metadata_rev}; refusing to remap it twice")
        outputs[path] = updated
        patch.extend(unified_diff(original.splitlines(True), updated.splitlines(True),
                                  fromfile="a/" + path, tofile="b/" + path))
        reports[arch] = {"counts": counts, "permutations": maps}
    # Verify both CPUs and all files before writing either CPU's plan.
    if options.patch:
        options.patch.write_text("".join(patch))
    if options.write:
        for path, updated in outputs.items():
            (ROOT / path).write_text(updated)
    print(json.dumps(reports, indent=2))


if __name__ == "__main__":
    main()

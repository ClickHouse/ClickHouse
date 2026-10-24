#!/usr/bin/env python3
"""Record CMake linker operands as references to Bazel compilation groups.

Example: python3 bazel/initial_migration/cmake_link_plan.py --reference /path/to/parity/ref
The reference must contain cmake-file-api.tar.gz, build.ninja.gz and
ninja-commands.txt.gz from the same configured build. The link compiler's
recorded --target chooses cmake_link_data.bzl or cmake_link_data_aarch64.bzl.
"""

import argparse
import ast
import gzip
import json
import os
from pathlib import Path
import re
import shlex
import subprocess

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.build_file import EMBEDDED_SOURCES, embedded_indices, native_source_owner, source_compile_label, target_label, target_owner
from initial_migration import llvm_targets
from initial_migration.reference_readers import read_file_api, ninja_edges, ninja_words as words
from initial_migration.repository_inputs import REPOSITORIES, ROOT
from initial_migration.rust_archives import ARCHIVE_TARGETS as RUST_ARCHIVES


def object_ranges(objects):
    """Encode adjacent indices of one compilation group as [ref, start, end]."""
    ranges = []
    for kind, ref, index in objects:
        if kind != "object":
            raise ValueError("Expected an object reference: " + repr((kind, ref, index)))
        if ranges and ranges[-1][0] == ref and ranges[-1][2] == index:
            ranges[-1][2] += 1
        else:
            ranges.append([ref, index, index + 1])
    return ranges


class Plan:
    def __init__(self, reference):
        self.reference = Path(reference)
        model, self.targets = read_file_api(self.reference / "cmake-file-api.tar.gz")
        self.source = model["paths"]["source"]
        self.build = model["paths"]["build"]
        self.owners = {}
        for name, target in self.targets.items():
            if name.startswith("LLVM") or name in llvm_targets.NATIVE_RUNTIMES:
                self.owners[name] = "contrib/llvm-project"
            elif name == "clickhouse":
                self.owners[name] = None
            else:
                self.owners[name] = target_owner(target)
        self.artifacts = {}
        for name, target in self.targets.items():
            for a in target.get("artifacts", []):
                path = os.path.normpath(a["path"])
                if path in self.artifacts and self.artifacts[path] != name:
                    raise ValueError("Multiple CMake owners: " + path)
                self.artifacts[path] = name
        self.rules = {}
        self.binary_vars = {}
        # Store only CMake build edges, not commands or the 130 MB of flags.
        for node in ninja_edges(
                self.reference / "build.ninja.gz",
                variables=("FLAGS", "LINK_FLAGS", "LINK_LIBRARIES", "LINK_PATH")):
            for output in node["outputs"] + node["implicit_outputs"]:
                self.rules[os.path.normpath(output)] = (node["rule"], node["explicit_inputs"])
            if "programs/clickhouse" in node["outputs"]:
                self.binary_vars = {key: words(value) for key, value in node["variables"].items()}
        self.refs = []
        self.expected = []
        self.ref_index = {}
        self.archives = {}
        self.archive_index = {}
        self.slots = {}
        self.embedded_groups = {}
        self.omitted = set()
        self.make_slots()
        if set(self.embedded_groups) != EMBEDDED_SOURCES.keys():
            raise ValueError('Missing CMake groups for the ClickHouse #embed objects')

    def ref(self, label, expected):
        if label in self.ref_index:
            n = self.ref_index[label]
            if self.expected[n] != expected:
                raise ValueError(f"Inconsistent object counts for {label}")
            return n
        index = len(self.refs)
        self.refs.append(label)
        self.expected.append(expected)
        self.ref_index[label] = index
        return index

    def source_name(self, ninja_path):
        absolute = os.path.normpath(os.path.join(self.build, ninja_path))
        prefix = self.source + "/"
        if not absolute.startswith(prefix):
            raise ValueError("Ninja source escapes CMake source tree: " + absolute)
        return absolute.removeprefix(prefix)

    def make_slots(self):
        for name, t in self.targets.items():
            actual_group = 0
            first = {}
            for g in t.get("compileGroups", []):
                srcs = [os.path.normpath(t["sources"][s]["path"]) for s in g["sourceIndexes"]]
                omitted = [s for s in srcs if s.endswith("/contrib/icu-cmake/empty.cpp")]
                self.omitted.update((name, s) for s in omitted)
                srcs = [s for s in srcs if s not in omitted]
                if not srcs:
                    continue
                label = target_label(name, self.owners) + "__compile_" + str(actual_group)
                owner = self.owners[name]
                partitions = {}
                for source in srcs:
                    src_owner = owner
                    if t['type'] != 'EXECUTABLE' and owner != 'contrib/llvm-project':
                        relative = source.removeprefix(self.source + '/')
                        src_owner = native_source_owner(relative, owner, g['language'])
                    partitions.setdefault(src_owner, []).append(source)
                for src_owner, group_sources in partitions.items():
                    group_label = label
                    if src_owner != owner:
                        first.setdefault(src_owner, actual_group)
                        group_label = source_compile_label(name, actual_group, src_owner, first[src_owner])
                    size = len(group_sources)
                    embedded = embedded_indices(name, [s.removeprefix(self.source + '/') for s in group_sources])
                    if embedded:
                        if name in self.embedded_groups or src_owner != owner or g['language'] != 'CXX':
                            raise ValueError(f'{name}: expected one native C++ group for its #embed objects')
                        self.embedded_groups[name] = group_label, group_sources, set(embedded)
                    for index, source in enumerate(group_sources):
                        key = (name, source)
                        if key in self.slots:
                            raise ValueError("Multiple compilation groups for " + str(key))
                        # Register only references reachable from the final link.
                        self.slots[key] = (group_label, index, size)
                actual_group += 1
            generated = []
            for source in t.get("sources", []):
                if source.get("isGenerated") and source["path"].endswith(".o"):
                    path = os.path.normpath(os.path.join(self.source, source["path"]))
                    if path.startswith(self.build + "/") and path.removeprefix(self.build + "/") in self.rules:
                        generated.append(path.removeprefix(self.build + "/"))
            for i, source in enumerate(generated):
                key = (name, source)
                if key in self.slots:
                    raise ValueError("Duplicate generated object: " + str(key))
                self.slots[key] = (target_label(name, self.owners) + "__compile_" + str(actual_group),
                                   i, len(generated))

    def split_embedded_slots(self):
        for name, (parent, sources, positions) in sorted(self.embedded_groups.items()):
            child = target_label(name + '_embedded', self.owners)
            self.ref(child, len(positions))
            remaining = len(sources) - len(positions)
            self.expected[self.ref_index[parent]] = remaining
            ordinary = embedded = 0
            for index, source in enumerate(sources):
                if index in positions:
                    self.slots[name, source] = child, embedded, len(positions)
                    embedded += 1
                else:
                    self.slots[name, source] = parent, ordinary, remaining
                    ordinary += 1
        self.embedded_groups.clear()

    def object(self, path, archive_owner=None):
        path = os.path.normpath(path)
        match = re.search(r"(?:^|/)CMakeFiles/([^/]+)[.]dir/", path)
        if not match:
            key = (archive_owner, path)
            if key not in self.slots:
                raise ValueError("Object has no CMake owner or declared generated output: " + str(key))
            label, index, size = self.slots[key]
            return ["object", self.ref(label, size), index]
        if match[1] not in self.targets:
            raise ValueError("Object has no CMake owner: " + path)
        name = match[1]
        if path not in self.rules:
            raise ValueError("Object has no Ninja compile edge: " + path)
        rule, inputs = self.rules[path]
        if len(inputs) != 1:
            raise ValueError(f"{path}: expected one source in {rule}, got {inputs}")
        key = (name, self.source_name(inputs[0]))
        if key in self.omitted:
            return None
        if key not in self.slots:
            raise ValueError(f"{path}: no CMake compilation group for {key} ({rule})")
        label, index, size = self.slots[key]
        return ["object", self.ref(label, size), index]

    def archive(self, path):
        path = os.path.normpath(path.removeprefix(self.build + "/"))
        if path in self.archive_index:
            return ["archive", self.archive_index[path]]
        index = len(self.archives)
        self.archive_index[path] = index
        self.archives[path] = None
        if path in RUST_ARCHIVES:
            name = RUST_ARCHIVES[path]
            obj = {"file": self.ref("//bazel/rust:" + name + "_cc", -1)}
            if name == "skim":
                obj["append"] = object_ranges([
                    ["object", self.ref("//bazel/rust:skim_bridge", 2), 0],
                    ["object", self.ref("//bazel/rust:skim_bridge", 2), 1],
                    ["object", self.ref("//bazel/rust:cxx_runtime", 1), 0],
                ])
        else:
            if path not in self.artifacts:
                raise ValueError("Archive has no File API owner: " + path)
            name = self.artifacts[path]
            if path not in self.rules:
                raise ValueError("Archive has no Ninja edge: " + path)
            rule, inputs = self.rules[path]
            if "STATIC_LIBRARY_LINKER" not in rule:
                raise ValueError(f"{path}: unexpected Ninja archive rule {rule}")
            objects = [self.object(v, name) for v in inputs]
            objects = [v for v in objects if v is not None]
            if not objects:
                raise ValueError("Archive has no Bazel compilation inputs: " + path)
            ref = objects[0][1]
            # A single, complete compilation group already has this archive.
            if ([v[1] for v in objects] == [ref] * len(objects)
                    and [v[2] for v in objects] == list(range(self.expected[ref]))):
                obj = {"file": ref}
            else:
                obj = {"objects": object_ranges(objects)}
        self.archives[path] = obj | {"name": path}
        return ["archive", index]

    def operand(self, arg):
        if arg.startswith("--gcc-toolchain="):
            return ["gcc"]
        if arg.startswith("--sysroot="):
            return ["sysroot"]
        if arg.startswith("--ld-path="):
            return ["lld"]
        if arg == "-llibllvmlibc":
            # Pass the exact archive File instead of a build-tree -L search.
            return self.archive("contrib/libllvmlibc-cmake/liblibllvmlibc.a")
        if arg.startswith("-L") and arg[2:].startswith(self.build + "/"):
            if os.path.normpath(arg[2:]) != self.build + "/contrib/libllvmlibc-cmake":
                raise ValueError("Unknown native link search directory: " + arg)
            return None
        if not arg.startswith("-") and arg.endswith(".a"):
            return self.archive(arg)
        if not arg.startswith("-") and arg.endswith(".o"):
            obj = self.object(arg)
            if obj is None:
                raise ValueError("CMake-only empty source in executable: " + arg)
            return obj
        # Retain -rpath exactly: it is stored ELF metadata, not an input.
        if arg.startswith("-Wl,-rpath,") or arg.startswith("-ffile-prefix-map="):
            return ["arg", arg]
        if self.source in arg or self.build in arg:
            raise ValueError("Untranslated native filesystem reference: " + arg)
        return ["arg", arg]

    def generate(self):
        with gzip.open(self.reference / "ninja-commands.txt.gz", "rt") as stream:
            last = None
            for line in stream:
                last = line
        command = shlex.split(last)
        if command[:2] != [":", "&&"]:
            raise ValueError("Missing final CMake link command")
        command = command[2:command.index("&&", 2)]
        if not command[0].endswith("/clang++"):
            raise ValueError("CMake link compiler is not clang++")
        if not command[-2:] == ["-o", "programs/clickhouse"]:
            raise ValueError("Unexpected CMake link output")
        native = []
        for arg in command[1:-2]:
            if arg == "@CMakeFiles/clickhouse.rsp":
                native += (self.rules["programs/clickhouse"][1]
                           + self.binary_vars["LINK_PATH"] + self.binary_vars["LINK_LIBRARIES"])
            else:
                native.append(arg)
        if len(native) < 1100:
            raise ValueError("CMake linker response was not expanded")
        # Verify the independently recorded File API archive order, including
        # duplicates and the two absolute compiler-rt paths before the rsp.
        api_archives = [
            os.path.normpath(w.removeprefix(self.build + "/"))
            for frag in self.targets["clickhouse"]["link"]["commandFragments"]
            for w in shlex.split(frag["fragment"]) if w.endswith(".a") and not w.startswith("-")
        ]
        ninja_archives = [
            os.path.normpath(w.removeprefix(self.build + "/"))
            for w in native if w.endswith(".a") and not w.startswith("-")
        ]
        if api_archives != ninja_archives:
            raise ValueError("Ninja and CMake File API archive orders differ")
        def operands():
            return [v for arg in native if (v := self.operand(arg)) is not None]
        sequence = operands()
        # Preserve existing CMake ref numbers: discover them first, append the two
        # new owners, then encode the same Ninja members with their new source slots.
        if self.embedded_groups:
            self.split_embedded_slots()
            self.archives.clear()
            self.archive_index.clear()
            sequence = operands()
        return {
            "refs": self.refs,
            "expected_objects": self.expected,
            "archives": list(self.archives.values()),
            "sequence": sequence,
        }

    def check_labels(self, builds=None):
        """Check archive owners and resolve collapsed single-compile libraries.

        The other CPU may have more compile groups than this reference. The
        generated BUILD declaration, not this CPU's File API alone, decides
        whether __compile_0 was collapsed into the original library.
        """
        repositories = {"@" + spec["name"]: owner for owner, spec in REPOSITORIES.items()}
        rust_names = {name + "_cc" for name in RUST_ARCHIVES.values()} | {"skim_bridge", "cxx_runtime"}
        declared = {}

        def declarations(owner, filename):
            key = owner if builds is not None else filename
            if key not in declared:
                if builds is not None:
                    declared[key] = dict.fromkeys(builds[owner].names) if owner in builds else {}
                else:
                    parsed = ast.parse(filename.read_text(), filename=str(filename))
                    declared[key] = {
                        keyword.value.value: getattr(statement.value.func, "id", "")
                        for statement in parsed.body
                        if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call)
                        for keyword in statement.value.keywords
                        if keyword.arg == "name" and isinstance(keyword.value, ast.Constant)
                    }
            return declared[key]

        def resolved_name(names, name):
            if name in names:
                return name
            collapsed = name.removesuffix("__compile_0")
            if collapsed != name and collapsed in names and names[collapsed] in ("ch_cc_library", None):
                return collapsed
            if (name == "_fastops__compile_1" and "_fastops_sve" in names
                    and names["_fastops_sve"] in ("ch_cc_library", None)):
                return "_fastops_sve"
            return None

        main_builds = None
        for index, label in enumerate(self.refs):
            if label.startswith("//bazel/rust:"):
                if label.removeprefix("//bazel/rust:") not in rust_names:
                    raise ValueError("No declared Rust archive or bridge: " + label)
                continue
            repository, rest = label.split("//", 1)
            package, name = rest.split(":", 1)
            if repository:
                if repository not in repositories:
                    raise ValueError("No declared Bazel repository: " + label)
                owner = repositories[repository]
                filename = ROOT / "bazel/dependencies" / (REPOSITORIES[owner]["name"] + ".BUILD.bazel")
            else:
                owner = None
                filename = ROOT / package / "BUILD.bazel"
            resolved = resolved_name(declarations(owner, filename), name)
            if resolved is None and builds is None and not repository and not package:
                # CMake supplies target names without Bazel packages. Git does
                # not recurse into the pinned contrib submodules.
                if main_builds is None:
                    files = subprocess.check_output([
                        "git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--",
                        "src/BUILD.bazel", "src/**/BUILD.bazel",
                        "base/BUILD.bazel", "base/**/BUILD.bazel",
                        "programs/BUILD.bazel", "programs/**/BUILD.bazel",
                        "contrib/BUILD.bazel", "contrib/**/BUILD.bazel",
                    ], cwd=ROOT).decode().split("\0")
                    main_builds = sorted({ROOT / path for path in files if path})
                matches = []
                for candidate in main_builds:
                    target = resolved_name(declarations(None, candidate), name)
                    if target is not None:
                        matches.append((candidate.parent.relative_to(ROOT).as_posix(), target))
                if len(matches) == 1:
                    package, resolved = matches[0]
                elif matches:
                    raise ValueError("Multiple Bazel targets for " + label + ": " + repr(matches))
            if resolved is None:
                raise ValueError("No Bazel target for " + label + " in " + str(filename))
            self.refs[index] = repository + "//" + package + ":" + resolved
        if len(set(self.refs)) != len(self.refs):
            raise ValueError("Collapsed compile libraries describe duplicate archive owners")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True)
    parser.add_argument("--output", type=Path, help="Override the output chosen from the CMake --target")
    opts = parser.parse_args()
    plan = Plan(opts.reference)
    data = plan.generate()
    plan.check_labels()
    target_args = [op[1] for op in data["sequence"] if op[0] == "arg" and op[1].startswith("--target=")]
    arches = {"--target=" + arch + "-linux-gnu": arch for arch in ("x86_64", "aarch64")}
    if len(target_args) != 1 or target_args[0] not in arches:
        raise ValueError("Expected one supported CMake link compiler target, got " + repr(target_args))
    arch = arches[target_args[0]]
    output = opts.output or ROOT / ("bazel/rules/cmake_link_data" + ("_aarch64" if arch == "aarch64" else "") + ".bzl")
    output.write_text(
        f"# Generated by bazel/initial_migration/cmake_link_plan.py from the {arch} CMake File API and Ninja.\n"
        '\"\"\"Ordered ClickHouse CMake link, using only references to Bazel artifacts.\"\"\"\n\n'
        "CMAKE_LINK = " + json.dumps(data, indent=2) + "\n"
    )
    kinds = {k: sum(k in a for a in data["archives"]) for k in ("file", "objects", "append")}
    print(f"{arch}: {len(data['sequence'])} link operands, {len(data['refs'])} Bazel refs; "
          f"{len(data['archives'])} unique archives {kinds}")


if __name__ == "__main__":
    main()

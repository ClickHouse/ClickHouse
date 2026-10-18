#!/usr/bin/env python3
"""Record CMake linker operands as references to Bazel compilation groups.

Example: python3 bazel/cmake_link_plan.py --reference /path/to/parity/ref
The reference must contain cmake-file-api.tar.gz, build.ninja.gz and
ninja-commands.txt.gz from the same configured build.
"""

import argparse
import gzip
import json
import os
from pathlib import Path
import re
import shlex
import tarfile

from build_file import target_label, target_owner
import llvm_targets
from repository_inputs import REPOSITORIES, ROOT

RUST_ARCHIVES = {
    "rust/workspace/lib_ch_rust_skim_rust.a": "skim",
    "rust/workspace/lib_ch_rust_prql.a": "prql",
    "rust/workspace/lib_ch_rust_polyglot.a": "polyglot",
    "rust/workspace/lib_ch_rust_pco.a": "pco",
    "rust/workspace/lib_ch_rust_wasmtime.a": "wasmtime",
    "contrib/delta-kernel-rs-cmake/libdelta_kernel_ffi.a": "delta_kernel_ffi",
    "contrib/chdig-cmake/libchdig.a": "chdig",
}

def words(text):
    # Paths in these references must not contain escaped Ninja separators.
    if "$ " in text or "$:" in text or "\n" in text.rstrip("\n"):
        raise ValueError("Unsupported Ninja escaping in link reference: " + text[:240])
    return shlex.split(text.replace("$$", "$"))


class Plan:
    def __init__(self, reference):
        self.reference = Path(reference)
        with tarfile.open(self.reference / "cmake-file-api.tar.gz") as tf:
            files = {Path(m.name).name: m for m in tf if m.isfile()}

            def read(name):
                return json.load(tf.extractfile(files[name]))

            index = read(sorted(n for n in files if n.startswith("index-"))[-1])
            model = read(next(o["jsonFile"] for o in index["objects"] if o["kind"] == "codemodel"))
            self.source = model["paths"]["source"]
            self.build = model["paths"]["build"]
            self.targets = {t["name"]: read(t["jsonFile"]) for t in model["configurations"][0]["targets"]}
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
        with gzip.open(self.reference / "build.ninja.gz", "rt") as stream:
            in_binary = False
            for line in stream:
                if line.startswith("build "):
                    in_binary = False
                    out, sep, rest = line[6:].partition(": ")
                    if not sep:
                        raise ValueError("Malformed Ninja build edge: " + line[:240])
                    tokens = words(rest)
                    rule, explicit = tokens[0], []
                    for token in tokens[1:]:
                        if token in {"|", "||", "|@"}:
                            break
                        explicit.append(token)
                    for output in words(out):
                        if output in {"|", "||"}:
                            continue
                        self.rules[os.path.normpath(output)] = (rule, explicit)
                    in_binary = "programs/clickhouse" in words(out)
                elif in_binary and line.startswith("  "):
                    key, sep, val = line.strip().partition(" = ")
                    if sep and key in {"FLAGS", "LINK_FLAGS", "LINK_LIBRARIES", "LINK_PATH"}:
                        self.binary_vars[key] = words(val)
                elif not line.startswith("  ") and line.strip():
                    in_binary = False
        self.refs = []
        self.expected = []
        self.ref_index = {}
        self.archives = {}
        self.archive_index = {}
        self.slots = {}
        self.omitted = set()
        self.make_slots()

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
            for g in t.get("compileGroups", []):
                srcs = [os.path.normpath(t["sources"][s]["path"]) for s in g["sourceIndexes"]]
                omitted = [s for s in srcs if s.endswith("/contrib/icu-cmake/empty.cpp")]
                self.omitted.update((name, s) for s in omitted)
                srcs = [s for s in srcs if s not in omitted]
                if not srcs:
                    continue
                label = target_label(name, self.owners) + "__compile_" + str(actual_group)
                for index, source in enumerate(srcs):
                    size = len(srcs)
                    key = (name, source)
                    if key in self.slots:
                        raise ValueError("Multiple compilation groups for " + str(key))
                    # Register only references reachable from the final link.
                    self.slots[key] = (label, index, size)
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
                obj["append"] = [
                    ["object", self.ref("//bazel/rust:skim_bridge", 2), 0],
                    ["object", self.ref("//bazel/rust:skim_bridge", 2), 1],
                    ["object", self.ref("//bazel/rust:cxx_runtime", 1), 0],
                ]
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
                obj = {"objects": objects}
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
        sequence = [self.operand(arg) for arg in native]
        sequence = [v for v in sequence if v is not None]
        return {
            "refs": self.refs,
            "expected_objects": self.expected,
            "archives": list(self.archives.values()),
            "sequence": sequence,
        }

    def check_labels(self):
        for label in self.refs:
            if label in {"//:clickhouse__compile_0", "//bazel/rust:skim_bridge", "//bazel/rust:cxx_runtime"}:
                continue
            if label.startswith("//bazel/rust:"):
                continue
            repository, rest = label.split("//", 1)
            if repository:
                spec = next((v for v in REPOSITORIES.values() if "@" + v["name"] == repository), None)
                if not spec:
                    raise ValueError("No declared Bazel repository: " + label)
                build = ROOT / "bazel/dependencies" / (spec["name"] + ".BUILD.bazel")
            else:
                build = ROOT / "BUILD.bazel"
            name = rest.removeprefix(":")
            if label == "@clickhouse_lz4//:_lz4__compile_0":
                # This CMake-only compilation group is declared by the overlay macro.
                if 'declare_lz4_cmake_objects()' in build.read_text():
                    continue
            if not re.search(r'\bname\s*=\s*"' + re.escape(name) + r'"', build.read_text()):
                raise ValueError("No Bazel target for " + label + " in " + str(build))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True)
    parser.add_argument("--output", type=Path, default=ROOT / "bazel/cmake_link_data.bzl")
    opts = parser.parse_args()
    plan = Plan(opts.reference)
    data = plan.generate()
    plan.check_labels()
    opts.output.write_text(
        "# Generated by bazel/cmake_link_plan.py from the x86_64 CMake File API and Ninja.\n"
        '\"\"\"Ordered ClickHouse CMake link, using only references to Bazel artifacts.\"\"\"\n\n'
        "CMAKE_LINK = json.decode(r\x27\x27\x27\n" + json.dumps(data, indent=2) + "\n\x27\x27\x27)\n"
    )
    kinds = {k: sum(k in a for a in data["archives"]) for k in ("file", "objects", "append")}
    print(f"{len(data['sequence'])} link operands, {len(data['refs'])} Bazel refs; "
          f"{len(data['archives'])} unique archives {kinds}")


if __name__ == "__main__":
    main()

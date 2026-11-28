#!/usr/bin/env python3
"""Scope CMake's command model to clickhouse and report the two command audits."""

import argparse
from collections import Counter
from dataclasses import replace
import json
from pathlib import Path
import re
import sys


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def expanded_arguments(action):
    """Expand only aquery's own parameter files, preserving argument boundaries.

    Bazel usually flattens ctx.actions.args() in CommandAction.getArguments()
    already. Separate ParameterFileWriteAction inputs can still appear as
    @execPath with a paramFiles record. Never infer contents from action inputs,
    response file names or CMake's link plan.
    """
    parameters = {}
    for entry in action.get("paramFiles", []):
        path = entry["execPath"]
        if path in parameters:
            raise ValueError(f"Duplicate aquery parameter file: {path}")
        parameters[path] = entry["arguments"]

    def expand(argv, active):
        result = []
        for arg in argv:
            if arg.startswith("-Wl,") and any(p.startswith("@") for p in arg[4:].split(",")):
                # paramFiles carries CommandAction arguments, not the contents
                # of a linker-created response file. Do not silently compare
                # the file name as if it were the link command.
                raise ValueError(f"{action.get('mnemonic')}: unresolved linker argument {arg}")
            if arg.startswith("@"):
                path = arg[1:]
                if path not in parameters:
                    raise ValueError(f"{action.get('mnemonic')}: unresolved argument {arg}")
                if path in active:
                    raise ValueError(f"Cyclic aquery parameter file: {path}")
                result.extend(expand(parameters[path], active | {path}))
            else:
                result.append(arg)
        return result

    return expand(action.get("arguments", []), set())


def adapt(directory):
    """Produce a neutral-mnemonic extractor input; retain the raw file and labels."""
    graph = json.loads((directory / "native.aquery.json").read_text())
    labels = {entry["id"]: entry["label"] for entry in graph.get("targets", [])}
    changed = []
    counts = Counter()
    for action in graph.get("actions", []):
        mnemonic = action.get("mnemonic")
        counts[mnemonic] += 1
        if mnemonic not in {"CppCompile", "ObjcCompile", "CppLink", "CppArchive",
                            "ClickHouseNativeLink", "ClickHouseNativeArchive",
                            "ClickHouseRustArchive", "ClickHouseRustSymbols"}:
            continue
        argv = expanded_arguments(action)
        if mnemonic == "ClickHouseNativeLink":
            # Only the real final link changes its neutral mnemonic. The custom
            # archives also belong to //:clickhouse; converting those into
            # CppArchive would incorrectly turn the executable into a library.
            if labels.get(action.get("targetId")) != "//:clickhouse":
                raise ValueError("Unexpected ClickHouseNativeLink owner")
            action["mnemonic"] = "CppLink"
        if argv != action.get("arguments", []) or action["mnemonic"] != mnemonic:
            changed.append({
                "label": labels.get(action.get("targetId")),
                "action_key": action.get("actionKey"),
                "from": mnemonic, "to": action["mnemonic"],
                "original_argument_count": len(action.get("arguments", [])),
                "expanded_argument_count": len(argv),
            })
        action["arguments"] = argv
    write_json(directory / "native.any2bazel.aquery.json", graph)
    write_json(directory / "any2bazel-adaptation.json", {
        "raw": "native.aquery.json",
        "extractor_input": "native.any2bazel.aquery.json",
        "actual_mnemonic_counts": dict(counts),
        "converted_actions": changed,
        "scope": "Response expansion and neutral final-link mnemonic only. Actual labels are retained.",
    })
    print("any2bazel extractor adaptation:", dict(counts), "changed:", len(changed))


def project(directory):
    """Make stock any2bazel associate the proven main object with its executable."""
    from model import TargetKind, TargetRole
    from serialize import load_model, dump_model

    actual = load_model(str(directory / "model.bazel.json"))
    root = actual.targets.get(":clickhouse")
    helper = actual.targets.get(":clickhouse__compile_0")
    transferred = []
    unlinked = []
    if root is not None and helper is not None:
        links = [a for a in root.actions if a.mnemonic == "CppLink"
                 and any(p.endswith("/bin/clickhouse") for p in a.outputs)]
        if len(links) != 1:
            raise ValueError(f"Expected one final link for main attribution: {len(links)}")
        link_operands = set(links[0].arguments)
        retained = []
        for action in helper.actions:
            outputs = {p for p in action.outputs if p.endswith(".o")}
            if action.mnemonic in {"CppCompile", "ObjcCompile"}:
                if outputs and outputs & link_operands:
                    transferred.append(action)
                    continue
                unlinked.append({"original_label": ":clickhouse__compile_0",
                                 "outputs": list(action.outputs)})
            retained.append(action)
        if transferred:
            actual.targets[":clickhouse"] = replace(
                root, actions=root.actions + transferred,
                kind=TargetKind.EXECUTABLE, role=TargetRole.PRODUCTION,
            )
            actual.targets[":clickhouse__compile_0"] = replace(helper, actions=retained)
    adaptation_path = directory / "any2bazel-adaptation.json"
    adaptation = json.loads(adaptation_path.read_text())
    adaptation["stock_only_main_attribution"] = [{
        "original_label": ":clickhouse__compile_0",
        "stock_label": ":clickhouse",
        "mnemonic": a.mnemonic,
        "outputs": list(a.outputs),
        "evidence": "Object is an explicit operand of the actual final link.",
    } for a in transferred]
    adaptation["unlinked_main_compiles"] = unlinked
    write_json(adaptation_path, adaptation)
    dump_model(actual, str(directory / "model.bazel.any2bazel.json"))
    print("any2bazel main attribution: linked =", len(transferred), "unlinked =", len(unlinked))


def scope(directory):
    model = json.loads((directory / "model.cmake.full.json").read_text())
    targets = model["targets"]
    if "clickhouse" not in targets:
        raise ValueError("CMake File API model has no clickhouse target")

    direct_owners = set()
    unowned_objects = []
    reading_inputs = False
    for line in (directory / "ninja-clickhouse-query.txt").read_text().splitlines():
        if line.startswith("  input:"):
            reading_inputs = True
        elif line.startswith("  outputs:"):
            reading_inputs = False
        elif reading_inputs and line.strip().endswith(".o"):
            owner = re.search(r"CMakeFiles/([^/]+)\.dir/", line)
            if owner:
                direct_owners.add(owner[1])
            else:
                unowned_objects.append(line.strip())
    absent = direct_owners - targets.keys()
    if absent or unowned_objects:
        raise ValueError("Ninja direct object inputs lack CMake targets: "
                         f"{sorted(absent)}, {unowned_objects}")

    kept = set()
    omitted_tools = set()
    pending = ["clickhouse", *sorted(direct_owners)]
    while pending:
        name = pending.pop()
        if name in kept or name in omitted_tools:
            continue
        target = targets[name]
        if name != "clickhouse" and target["kind"] == "executable":
            # These executables generate clickhouse sources; the selected
            # binary's output configuration does not contain their links.
            omitted_tools.add(name)
            continue
        kept.add(name)
        pending.extend(dep["name"] for dep in target["deps"] if dep["name"] in targets)

    inventory = {
        "bazel_target": "//:clickhouse",
        "cmake_target": "clickhouse",
        "ninja_direct_object_owners": sorted(direct_owners),
        "compared_cmake_targets": sorted(kept),
        "cmake_executable_build_tools_not_compared": sorted(omitted_tools),
        "cmake_targets_outside_clickhouse_closure": sorted(targets.keys() - kept - omitted_tools),
        "limits": [
            "Only CMake clickhouse and Bazel //:clickhouse are requested.",
            "The full unmodified CMake extraction is model.cmake.full.json.",
            "CMake-generated executable build tools are not binary targets in this comparison.",
        ],
    }
    write_json(directory / "scope.json", inventory)
    model["targets"] = {name: value for name, value in targets.items() if name in kept}
    write_json(directory / "model.cmake.clickhouse.json", model)


def report(directory, native_status):
    stock = json.loads((directory / "stock-diff.json").read_text())
    native_path = directory / "native-actions.json"
    if native_status not in (0, 1) or not native_path.is_file():
        raise RuntimeError(f"audit_native_actions.py failed with status {native_status}; "
                           "see native-actions.log")
    native = json.loads(native_path.read_text())
    if native_status != (0 if native["converged"] else 1):
        raise RuntimeError("audit_native_actions.py status does not match native-actions.json")
    summary = {
        "target": "//:clickhouse",
        "stock_any2bazel": {
            key: stock[key] for key in ("converged", "errors", "warnings")
        },
        "native_actions": {
            "converged": native["converged"],
            "reference_compile_actions": native["reference_compile_actions"],
            "matched_compile_actions": native["matched_compile_actions"],
            "assembly_actions": native["assembly_actions"],
            "missing_actions": len(native["missing_actions"]),
            "difference_counts": native["difference_counts"],
            "clickhouse_link_closure": native["link_closures"]["clickhouse"]["passed"],
            "unresolved_link_operands": len(native["link_closures"]["clickhouse"]["unresolved_operands"]),
        },
        "binary_equality_verified": False,
        "limits": [
            "any2bazel ignores optimization/debug/sysroot differences and tests include presence, not order.",
            "audit_native_actions follows actual ar/MRI operands to required native objects and processed Rust imports.",
            "A processed Rust import proves its provider and operand, not the identity of Rust member bytes or flags.",
            "Neither audit proves ordered command identity, generated input identity or Rust compilation identity.",
            "Compare both complete ClickHouse executables with sha256sum and cmp before claiming binary equality.",
        ],
    }
    write_json(directory / "summary.json", summary)
    print(json.dumps(summary, indent=2))
    return 0 if stock["converged"] and native["converged"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("scope", "adapt", "project", "report"))
    parser.add_argument("directory", type=Path)
    parser.add_argument("--any2bazel", type=Path)
    parser.add_argument("--native-status", type=int)
    args = parser.parse_args()
    if args.operation == "scope":
        scope(args.directory)
        return 0
    if args.operation == "adapt":
        adapt(args.directory)
        return 0
    if args.operation == "project":
        if args.any2bazel is None:
            parser.error("project requires --any2bazel")
        sys.path.insert(0, str(args.any2bazel / "scripts"))
        project(args.directory)
        return 0
    if args.native_status is None:
        parser.error("report requires --native-status")
    return report(args.directory, args.native_status)


if __name__ == "__main__":
    raise SystemExit(main())

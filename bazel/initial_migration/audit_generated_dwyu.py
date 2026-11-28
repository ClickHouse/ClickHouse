#!/usr/bin/env python3
"""Check owned protobuf C++ and LLVM runtime compilers in one existing remote batch.

Discover the generated rules in the three production packages before selecting
the target platform. Use --enumerate-only to list them without building.
The imports section checks CcInfo against original archive actions and C++
consumer inputs. It does not classify Rust as C++. LLVM .S preprocessing is
checked against original Assemble actions.
"""

import argparse
import json
from pathlib import Path
import sys

from audit_full_dwyu import (
    Audit, FIELDS, RuntimeActionGraph, add_actions, audit_runtime_bindings,
    canonical, census_canonical, configured_analyzers,
    query_labels, read_cquery, runtime_cpp_candidate, runtime_asm_candidate, runtime_compile_candidate, sha_file,
    validate_compiler_coverage, validate_link_evidence,
    validate_runtime_compiler_inputs, write_json,
)
from audit_proto_dwyu import ProtoBep, audit_proto
from audit_rust_import_dwyu import check_imports


PLATFORM_QUERY = Path("bazel/initial_migration/generated_platform.cquery")
DWYU = "--aspects=//bazel/rules:dwyu.bzl%dwyu"
GROUPS = {
    "protobuf_generated_cc": ("cc_proto_library", "//bazel"),
    "llvm_runtime_compile_without_CcInfo": ("llvm_runtime_objects", "@clickhouse_llvm_project//"),
    "precompiled_cc_import": ("cc_import", "//bazel/rust"),
}
COMPILE_ACTIONS = (
    "CppCompile", "CppCompileHeader", "CppModuleCompile", "Assemble",
)
ARTIFACT_OPTIONS = (
    "--include_aspects", "--include_artifacts", "--include_commandline",
    "--include_param_files",
)


def inventory(audit):
    # A loading query includes incompatible targets and rules declared by macros.
    query = audit.out / "generated-declarations.query"
    query.write_text(" union ".join(
        f"kind('^{kind} rule$', '{package}:all')" for kind, package in GROUPS.values()
    ) + "\n")
    run = audit.command("generated-declarations", "query", [
        "--query_file=" + str(query), "--consistent_labels", "--output=label_kind",
    ])
    if run["exit_code"]:
        raise RuntimeError("generated declarations query failed: " + run["log"]["file"])

    groups = {(kind, census_canonical(package + ":")): group
              for group, (kind, package) in GROUPS.items()}
    declared = {}
    for line in Path(run["stdout"]["file"]).read_text().splitlines():
        kind, rule, name = line.split()
        label = canonical(name)
        query_labels([label])
        group = groups.get((kind, label.rpartition(":")[0] + ":"))
        if rule != "rule" or group is None or label in declared:
            raise ValueError("unexpected generated declaration: " + line)
        declared[label] = {"label": label, "kind": kind, "group": group}
    if {row["group"] for row in declared.values()} != set(GROUPS):
        raise ValueError("generated declarations query omitted a production rule group")
    return declared, run["stdout"]


def platform_observations(audit, expr, rows, name="generated-platform"):
    query = audit.out / (name + ".query")
    query.write_text(expr + "\n")
    run = audit.command(name, "cquery", [
        "--query_file=" + str(query), "--output=starlark", "--consistent_labels",
        "--starlark:file=" + str(PLATFORM_QUERY.resolve()),
    ], output=True)
    if run["exit_code"]:
        raise RuntimeError("generated platform cquery failed: " + run["log"]["file"])
    by_label = {}
    for key, row in rows.items():
        if row["label"] in by_label:
            raise ValueError("generated top-level configuration ambiguous: " + row["label"])
        by_label[row["label"]] = key
    found = {}
    for line in Path(run["stdout"]["file"]).read_text().splitlines():
        value = json.loads(line)
        if (not isinstance(value, dict) or set(value) != {"label", "incompatible", "cc_info"} or
                type(value["incompatible"]) is not bool):
            raise ValueError("invalid generated platform observation: " + repr(value))
        label = canonical(value["label"])
        if label not in by_label or by_label[label] in found:
            raise ValueError("unmatched or duplicate generated platform observation: " + label)
        found[by_label[label]] = value
    if set(found) != set(rows):
        raise ValueError("generated platform observation omitted original cquery rows")
    return found, run["stdout"]


def check_runtime(audit, keys, rows, observations, graph, ordinary, execroot):
    results, eligible = {}, {}
    for key in keys:
        row = rows[key]
        actions = graph.owners.get(key, [])
        record = {
            "label": row["label"], "checksum": row["checksum"],
            "language": row["language"], "sources": row["sources"],
            "actions": dict(row["actions"]), "cpp_qualified": False, "asm_qualified": False,
            "status": "not_proved",
        }
        results[key] = record
        if observations[key]["cc_info"] is not None:
            record["reason"] = "llvm_runtime_objects unexpectedly exported CcInfo"
        elif key not in ordinary.successful_roots:
            record["reason"] = "no successful ordinary original runtime root"
        elif any(value is None for value in row["sources"].values()):
            record["reason"] = "unconfigured runtime sources"
        elif not actions:
            # Private headers count as srcs, but llvm_runtime_objects returns
            # empty DefaultInfo after classifying them. The actual original
            # action query plus successful root build proves there is no TU.
            if row["actions"]:
                record["reason"] = "runtime group has actions outside original compiler inventory"
            else:
                record["status"] = "empty_no_cpp_or_assembly"
        elif runtime_compile_candidate(row):
            # audit_runtime_bindings validates every original filename,
            # command, and output. A raw .s is not eligible for .S PP.
            eligible[key] = row
        else:
            record["reason"] = "runtime produced unsupported C/C++ or assembly actions"
    if not eligible:
        return results
    labels = [row["label"] for row in eligible.values()]
    scope = query_labels(labels)
    analyzers = audit.query("generated-runtime-analyzers", "aquery",
        "outputs('.*(_dwyu_report[.]json|[.]dwyu_owned_headers[.]json)', " + scope + ")",
        (DWYU, "--include_aspects", "--include_artifacts", "--include_file_write_contents",
         "--include_commandline", "--include_param_files", "--output_groups=dwyu"))
    owners, headers = configured_analyzers(analyzers)
    evidence = {"analyzers": sha_file(analyzers)}
    bindings = audit_runtime_bindings(audit, labels, rows, eligible, headers, DWYU, evidence)
    if (set(owners) - set(eligible) or set(headers) - set(eligible) or
            evidence.get("runtime_preprocessors_not_eligible")):
        raise ValueError("runtime DWYU produced actions outside eligible original production owners")
    strict_bep = audit.out / "generated-runtime-strict.bep.json"
    strict = audit.command("generated-runtime-strict", "build", [
        "--keep_going", DWYU, "--output_groups=dwyu", "--remote_download_outputs=toplevel",
        "--build_event_publish_all_actions", "--build_event_json_file=" + str(strict_bep),
    ] + labels)
    bep = ProtoBep(strict_bep, labels, rows, "dwyu")
    for key in keys:
        record = results[key]
        if key not in eligible:
            if rows[key].get("runtime_source_binding"):
                record["reason"] = rows[key]["runtime_source_binding"]
            continue
        record["queries"] = evidence
        # These queries describe the original compilation even if DWYU fails.
        # A binding and a build attempt are not successful DWYU evidence.
        record["original_compiler_binding"] = bindings[key]
        outputs = owners.get(key, [])
        header_manifests = headers.get(key, [])
        report_output = outputs[0] if len(outputs) == 1 else None
        report_actions = bep.actions.get((key, report_output), []) if report_output else []
        record["strict_attempt"] = {
            "bep": bep.path,
            "invocation_uuid": bep.invocation_uuid,
            "build_exit_code": strict["exit_code"],
            "configured_owner": key,
            "successful_root": key in bep.successful_roots,
            "analyzer_count": len(outputs),
            "header_manifest_count": len(header_manifests),
            "report_output": report_output,
            "report_action_count": len(report_actions),
            "failed_report_action_count": sum(action.get("success") is not True for action in report_actions),
        }
        try:
            if len(outputs) != 1:
                raise ValueError("runtime expected one configured DWYU analyzer; got " + str(len(outputs)))
            if len(header_manifests) != 1:
                raise ValueError("runtime expected one configured header manifest; got " + str(len(header_manifests)))
            if bindings[key].get("status") != "matched_original_actions":
                raise ValueError("runtime PP did not match original compilation: " + repr(bindings[key]))
            if key not in bep.successful_roots:
                raise ValueError("runtime original DWYU root did not complete successfully for " + key)
            value, report_evidence = bep.read_report(execroot, key, report_output)
            if canonical(value.get("analyzed_target", "")) != rows[key]["label"]:
                raise ValueError("DWYU report owner differs from original configured runtime")
            if any(not isinstance(value.get(field), kind) for field, kind in FIELDS.items()):
                raise ValueError("DWYU runtime report fields missing")
            validate_link_evidence(value)
            validate_compiler_coverage(value)
            inputs = validate_runtime_compiler_inputs(value, header_manifests[0]["contents"])
            record.update({
                "dwyu": report_evidence,
                "findings": {field: value[field] for field in FIELDS},
                "unverified_compiler_inputs": inputs,
                "unverified_compilation_deps": value["unverified_compilation_deps"],
                "unentered_owned_files": value["unentered_owned_files"],
                "unverified_link_deps": value["unverified_link_deps"],
            })
            clean = not (inputs or value["unverified_compilation_deps"] or
                         value["unentered_owned_files"] or value["unverified_link_deps"] or
                         value.get("unchecked_link_inputs") or any(value[field] for field in FIELDS))
            record["status"] = "clean" if clean else "findings"
            record["cpp_qualified"] = clean and runtime_cpp_candidate(rows[key])
            record["asm_qualified"] = clean and runtime_asm_candidate(rows[key])
        except (KeyError, TypeError, ValueError, OSError) as exc:
            record["reason"] = str(exc)
    return results


def run(args):
    audit = Audit(args)
    declared, source = inventory(audit)
    if args.enumerate_only:
        print(source["file"])
        return 0
    result = {
        "kind": "GENERATED_PRODUCTION_DWYU", "schema": 1,
        "config": args.config, "inventory": source,
        "runs": audit.runs, "declared": declared, "incompatible": {},
        "protobuf": {}, "runtime": {}, "imports": {}, "errors": {},
        "cpp_qualified": False, "asm_qualified": False, "whole_repository_clean": False,
        "imports_checked": False,
        "native_qualified": False, "failed_actions_cached": False,
    }
    try:
        requested = [item["label"] for item in declared.values()]
        expr = "config(" + query_labels(requested) + ", target)"
        original = audit.query("generated-original-roots", "cquery", expr)
        top, omitted = read_cquery(original)
        actual = {row["label"] for row in top.values()}
        if actual != set(declared) or omitted or any(row["is_tool"] for row in top.values()):
            raise ValueError("original generated roots omitted/added declarations or returned tool configurations")
        for row in top.values():
            if row["rule_class"] != declared[row["label"]]["kind"]:
                raise ValueError("generated query kind differs from configured rule: " + row["label"])
        observations, evidence = platform_observations(audit, expr, top)
        result["queries"] = {"roots": sha_file(original), "platforms": evidence}
        for key, value in observations.items():
            if value["incompatible"]:
                result["incompatible"][key] = {"label": value["label"],
                    "evidence": evidence, "cpp_qualified": False}
        compatible = {key: row for key, row in top.items() if key not in result["incompatible"]}
        labels = [declared[row["label"]]["label"] for row in compatible.values()]
        if not labels:
            raise ValueError("no compatible original production generated targets")
        closure = audit.query("generated-original-closure", "cquery", "deps(" + query_labels(labels) + ")")
        rows, unused_files = read_cquery(closure)
        result["queries"]["closure"] = sha_file(closure)
        if set(compatible) - set(rows):
            raise ValueError("standalone generated configurations were not preserved in original closure")
        # The protobuf aspect owns its actions on proto_library, not on the
        # cc_proto_library forwarding rules. audit_proto inspects those owners.
        compile_labels = [declared[row["label"]]["label"] for row in compatible.values()
                          if declared[row["label"]]["group"] != "protobuf_generated_cc"]
        action_path = audit.query("generated-original-root-actions", "aquery",
            "mnemonic('^(CppCompile|CppCompileHeader|CppModuleCompile|Assemble)$', " +
            query_labels(compile_labels) + ")",
            ARTIFACT_OPTIONS)
        unmatched = add_actions(action_path, rows)
        if unmatched:
            raise ValueError("original generated compiler actions not in original cquery: " + repr(unmatched))
        graph = RuntimeActionGraph(action_path, COMPILE_ACTIONS, rows, ("llvm_runtime_objects", "cc_import"))
        result["queries"]["original_actions"] = sha_file(action_path)
        native_bep = audit.out / "generated-ordinary.bep.json"
        audit.command("generated-ordinary", "build", [
            "--keep_going", "--remote_download_outputs=minimal", "--build_event_publish_all_actions",
            "--build_event_json_file=" + str(native_bep),
        ] + labels)
        ordinary = ProtoBep(native_bep, labels, rows)
        result["ordinary_bep"] = ordinary.path
        result["ordinary_invocation_uuid"] = ordinary.invocation_uuid
        info = audit.command("generated-execution-root", "info", ["execution_root"])
        if info["exit_code"]:
            raise RuntimeError("cannot read generated execution root")
        execroot = Path(Path(info["stdout"]["file"]).read_text().strip())
        groups = {
            name: {key: rows[key] for key, row in compatible.items()
                   if declared[row["label"]]["group"] == name}
            for name in GROUPS
        }
        # Imports consume the scheduled original CPP audits, never another
        # CPP build. Missing CPP manifests leave imports explicitly unchecked.
        try:
            result["imports"] = check_imports(
                audit, groups["precompiled_cc_import"], rows, observations, ordinary,
                platform_observations, ARTIFACT_OPTIONS, args.cpp_manifest)
        except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
            result["errors"]["imports"] = str(exc)
        try:
            result["runtime"] = check_runtime(
                audit, groups["llvm_runtime_compile_without_CcInfo"], rows, observations,
                graph, ordinary, execroot)
        except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
            result["errors"]["runtime"] = str(exc)
        try:
            selected = groups["protobuf_generated_cc"]
            if selected:
                result["protobuf"] = audit_proto(audit, labels, rows, selected, native_bep, execroot)
            else:
                result["errors"]["protobuf"] = "no compatible production protobuf roots"
        except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
            result["errors"]["protobuf"] = str(exc)
        scored = (set(result["runtime"]) | set(result["imports"]) |
                  set(result["protobuf"].get("selected_original_cc_proto", [])))
        missing = set(compatible) - scored
        excess = scored - set(compatible)
        result["missing_configured_owners"] = sorted(missing)
        result["unexpected_configured_owners"] = sorted(excess)
        common = (not result["errors"] and not missing and not excess and
                  all(v["language"] in ("C", "CXX", "ASM") for v in result["runtime"].values()))
        result["cpp_qualified"] = bool(common and result["protobuf"].get("scope_complete") is True and
            all(v["cpp_qualified"] or v["status"] == "empty_no_cpp_or_assembly"
                for v in result["runtime"].values() if v["language"] in ("C", "CXX")))
        result["remaining_assembly"] = [
            key for key, value in result["runtime"].items()
            if value["language"] == "ASM" and not value["asm_qualified"]
            and value["status"] != "empty_no_cpp_or_assembly"]
        result["asm_qualified"] = bool(common and not result["remaining_assembly"])
        result["remaining_import_headers"] = [
            key for key, value in result["imports"].items()
            if value.get("headers_applicable", True) and not value["headers_qualified"]]
        result["remaining_imports"] = [
            key for key, value in result["imports"].items() if not value["imports_checked"]]
        result["imports_checked"] = bool(
            set(result["imports"]) == set(groups["precompiled_cc_import"])
            and result["imports"] and not result["remaining_imports"])
    except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
        result["errors"]["inventory_or_original"] = str(exc)
    path = audit.out / "generated-manifest.json"
    write_json(path, result)
    print("GENERATED_DWYU_RESULT=" + json.dumps({
        **sha_file(path), "cpp_qualified": result["cpp_qualified"], "asm_qualified": result["asm_qualified"],
        "imports_checked": result["imports_checked"],
        "whole_repository_clean": False, "errors": result["errors"],
    }, sort_keys=True), flush=True)
    return 0 if result["cpp_qualified"] and result["asm_qualified"] and result["imports_checked"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, choices=("cmake_match", "cmake_match_aarch64"))
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--enumerate-only", action="store_true", help="list expanded generated rules without building")
    parser.add_argument("--cpp-manifest", action="append", type=Path, default=[],
                        help="completed original CPP census manifests for the requested platform")
    args = parser.parse_args()
    if Path(".bazelversion").read_text().strip() != "9.3.0":
        parser.error("generated production audit requires the recorded Bazel 9.3.0 action schema")
    return run(args)


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Audit the configured C/C++ owners reachable from explicit Bazel roots.

Run outside Bazel, from the workspace root. Raw queries and BEPs stay in
--out; manifest.json is keyed by (canonical label, configuration checksum).
"""

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time


NATIVE_CC = {"cc_library", "cc_binary", "cc_test"}
SOURCE_ATTRS = ("srcs", "hdrs", "textual_hdrs")
FIELDS = {
    "public_includes_without_dep": dict,
    "private_includes_without_dep": dict,
    "unused_deps": list,
    "unused_implementation_deps": list,
    "deps_which_should_be_private": list,
}
# These report unchecked owners, never successful DWYU. Non-Cc generators
# remain in the query summary and do not need a C/C++ header audit.
OTHER_CC = {"cc_import", "cc_proto_library", "cc_shared_library",
            "rust_import", "rust_library", "rust_static_library"}


def sha_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return {"file": str(path), "sha256": digest.hexdigest(), "bytes": path.stat().st_size}


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def canonical(label):
    # --consistent_labels uses Starlark str(Label) and BEP uses @@ for main.
    if label.startswith("@@//"):
        return label[2:]
    return label


def identity(label, checksum):
    return canonical(label) + " (" + checksum + ")"


def validate_link_evidence(report):
    """Require compiler-mode result fields; evidence never counts as a finding."""
    for field in ("unverified_link_deps", "unchecked_link_inputs"):
        if field in report and (not isinstance(report[field], list) or any(
            not isinstance(value, str) or not value for value in report[field]
        )):
            raise ValueError("invalid " + field)
    owned = report.get("direct_owned_link_symbols")
    forwarded = report.get("forwarded_link_symbols")
    if not isinstance(owned, dict) or not isinstance(forwarded, dict):
        raise ValueError("missing or invalid DWYU link evidence")
    for dep, symbols in owned.items():
        if (not isinstance(dep, str) or not dep or not isinstance(symbols, list) or not symbols or
            any(not isinstance(symbol, str) or not symbol for symbol in symbols) or len(set(symbols)) != len(symbols)):
            raise ValueError("invalid owned link evidence")
    for dep, records in forwarded.items():
        if not isinstance(dep, str) or not dep or not isinstance(records, list) or not records:
            raise ValueError("invalid forwarded link dependency")
        seen = set()
        for item in records:
            if (not isinstance(item, dict) or set(item) != {"actual_owner", "object", "symbol"} or any(
                not isinstance(item[field], str) or not item[field] for field in item
            ) or canonical(item["actual_owner"]) == canonical(dep)):
                raise ValueError("invalid forwarded link owner, object or symbol")
            key = item["actual_owner"], item["object"], item["symbol"]
            if key in seen:
                raise ValueError("duplicate forwarded link evidence")
            seen.add(key)
    reasons = report.get("link_input_reasons")
    if not isinstance(reasons, list):
        raise ValueError("missing or invalid DWYU link input reasons")
    fileless = {"user_link_flags", "linkstamps"}
    file_reasons = {
        "additional_link_input", "unchecked_object_directory", "static_archive",
        "unreadable_archive", "thin_archive", "unsupported_archive",
        "malformed_archive_nm", "malformed_object_nm", "dynamic_or_interface_library", "alwayslink",
        "unsupported_nm_kind",
    }
    occurrences = {}
    for item in reasons:
        if (not isinstance(item, dict) or set(item) != {"owner", "file", "reason", "routes"} or
            not isinstance(item["owner"], str) or not item["owner"] or
            not isinstance(item["file"], str) or not isinstance(item["reason"], str) or
            item["reason"] not in fileless | file_reasons or
            (item["reason"] in fileless) != (item["file"] == "") or
            not isinstance(item["routes"], list)):
            raise ValueError("invalid DWYU link input reason")
        seen = set()
        for route in item["routes"]:
            if (not isinstance(route, dict) or set(route) != {"attribute", "index", "target"} or
                route["attribute"] not in ("deps", "implementation_deps") or
                type(route["index"]) is not int or route["index"] < 0 or
                not isinstance(route["target"], str) or not route["target"]):
                raise ValueError("invalid DWYU link input route")
            key = route["attribute"], route["index"]
            if key in seen or (key in occurrences and occurrences[key] != route["target"]):
                raise ValueError("duplicate or inconsistent DWYU link input route")
            seen.add(key)
            occurrences[key] = route["target"]


def configuration_map(data, field):
    return {str(config["id"]): config for config in data.get(field, [])}


def read_cquery(path):
    data = json.loads(path.read_bytes())
    configs = configuration_map(data, "configurations")
    rules = {}
    skipped = Counter()
    for item in data.get("results", []):
        rule = item.get("target", {}).get("rule")
        if rule is None:
            skipped[item.get("target", {}).get("type", "no_target_type")] += 1
            continue
        cfg_id = str(item.get("configurationId", 0))
        if cfg_id == "0":
            skipped["unconfigured_rule:" + rule["ruleClass"]] += 1
            continue
        config = configs[cfg_id]
        key = identity(rule["name"], config["checksum"])
        attrs = {attr["name"]: attr for attr in rule.get("attribute", [])}
        source_counts = {}
        for name in SOURCE_ATTRS:
            attr = attrs.get(name, {})
            source_counts[name] = len(attr.get("stringListValue", []))
            if "selectorList" in attr:
                # A configured proto should resolve selects. Do not assert
                # an empty declared source list if a future Bazel fails to.
                source_counts[name] = None
        rules[key] = {
            "label": canonical(rule["name"]),
            "checksum": config["checksum"], "mnemonic": config.get("mnemonic"),
            "is_tool": config.get("isTool", False), "rule_class": rule["ruleClass"],
            "sources": source_counts,
            "tags": attrs.get("tags", {}).get("stringListValue", []),
            "features": attrs.get("features", {}).get("stringListValue", []),
            "actions": {},
            "configured_inputs": rule.get("configuredRuleInput", []),
            "dependency_attributes": {
                name: attrs[name] for name in ("deps", "implementation_deps", "refs", "actual")
                if name in attrs
            },
        }
    return rules, dict(sorted(skipped.items()))


def add_actions(path, rules):
    data = json.loads(path.read_bytes())
    configs = configuration_map(data, "configuration")
    targets = {str(target["id"]): target for target in data.get("targets", [])}
    classes = {str(rule["id"]): rule["name"] for rule in data.get("ruleClasses", [])}
    missing = Counter()
    for action in data.get("actions", []):
        target = targets[str(action["targetId"])]
        config = configs.get(str(action.get("configurationId", 0)))
        if config is None:
            missing["no_configuration:" + action["mnemonic"]] += 1
            continue
        key = identity(target["label"], config["checksum"])
        if key not in rules:
            missing[key + ":" + action["mnemonic"]] += 1
            continue
        row = rules[key]
        if row["rule_class"] != classes[str(target["ruleClassId"])]:
            raise ValueError("cquery/aquery rule class mismatch: " + key)
        if row["is_tool"] != config.get("isTool", False):
            raise ValueError("cquery/aquery tool configuration mismatch: " + key)
        mnemonic = action["mnemonic"]
        row["actions"][mnemonic] = row["actions"].get(mnemonic, 0) + 1
    return dict(sorted(missing.items()))


def query_labels(labels):
    if any(not re.fullmatch(r"(@{0,2}[a-zA-Z0-9_.+~-]*)?//[^\s'\"()]*", l) for l in labels):
        raise ValueError("not an explicit Bazel label: " + repr(labels))
    return "set(" + " ".join(labels) + ")"


class Audit:
    def __init__(self, args):
        self.args = args
        self.out = args.out.resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        self.bazel = ["bazel", "--host_jvm_args=-Xmx20g"]
        self.options = ["--config=" + args.config, "--noannounce_rc", "--color=no",
                        "--curses=no", "--show_progress_rate_limit=30"]
        self.runs = []

    def command(self, name, subcommand, extra, output=False):
        stdout = self.out / (name + (".json" if output else ".stdout"))
        log = self.out / (name + ".log")
        command = self.bazel + [subcommand] + self.options + extra
        start = time.monotonic()
        print("FULL_DWYU_BEGIN=" + json.dumps({"name": name, "command": command}), flush=True)
        with stdout.open("wb") as out, log.open("wb") as err:
            proc = subprocess.Popen(command, stdout=out, stderr=err)
            while True:
                try:
                    rc = proc.wait(timeout=45)
                    break
                except subprocess.TimeoutExpired:
                    print("FULL_DWYU_PROGRESS=" + json.dumps({
                        "name": name, "seconds": int(time.monotonic() - start),
                        "log_bytes": log.stat().st_size}), flush=True)
        run = {"name": name, "command": command, "exit_code": rc,
               "seconds": round(time.monotonic() - start, 2),
               "stdout": sha_file(stdout), "log": sha_file(log)}
        self.runs.append(run)
        print("FULL_DWYU_RUN=" + json.dumps(run, sort_keys=True), flush=True)
        return run

    def query(self, name, subcommand, expr, extra=()):
        query = self.out / (name + ".query")
        query.write_text(expr + "\n")
        opts = ["--query_file=" + str(query), "--output=jsonproto", "--consistent_labels"]
        if subcommand == "cquery":
            opts += ["--proto:include_configurations",
                     "--transitions=lite", "--noproto:locations",
                     "--noproto:rule_inputs_and_outputs",
                     "--proto:output_rule_attrs=srcs,hdrs,textual_hdrs,tags,features,deps,implementation_deps,refs,actual"]
        else:
            opts += ["--noinclude_artifacts", "--noinclude_commandline",
                     "--noinclude_param_files", "--noinclude_file_write_contents"]
        run = self.command(name, subcommand, opts + list(extra), output=True)
        if run["exit_code"]:
            raise RuntimeError(subcommand + " failed: " + run["log"]["file"])
        return Path(run["stdout"]["file"])

    def build(self, name, labels, strict):
        patterns = self.out / (name + ".targets")
        patterns.write_text("\n".join(sorted(set(labels))) + "\n")
        bep = self.out / (name + ".bep.json")
        extra = ["--keep_going", "--target_pattern_file=" + str(patterns),
                 "--build_event_json_file=" + str(bep)]
        if strict:
            extra += ["--config=dwyu", "--remote_download_outputs=toplevel"]
        else:
            extra += ["--remote_download_outputs=minimal"]
        run = self.command(name, "build", extra)
        events = defaultdict(list)
        filesets = {}
        if not bep.exists():
            run["bep_problem"] = "missing"
            return run, events, filesets
        run["bep"] = sha_file(bep)
        run["invocation_uuid"] = None
        with bep.open() as file:
            for line in file:
                event = json.loads(line)
                if "started" in event:
                    run["invocation_uuid"] = event["started"].get("uuid")
                if "namedSet" in event.get("id", {}):
                    filesets[event["id"]["namedSet"]["id"]] = event["namedSetOfFiles"]
                completed = event.get("id", {}).get("targetCompleted")
                if completed:
                    key = identity(completed["label"], completed.get("configuration", {}).get("id", ""))
                    events[key].append(event)
        return run, events, filesets


def configured_analyzers(path):
    """Map actual analyzed owner/checksum to Bazel's declared report artifact."""
    data = json.loads(path.read_bytes())
    configs = configuration_map(data, "configuration")
    targets = {str(t["id"]): t for t in data.get("targets", [])}
    fragments = {str(f["id"]): f for f in data.get("pathFragments", [])}
    artifacts = {}
    for artifact in data.get("artifacts", []):
        parts = []
        current = str(artifact["pathFragmentId"])
        while current in fragments:
            fragment = fragments[current]
            parts.append(fragment["label"])
            current = str(fragment.get("parentId", 0))
        artifacts[str(artifact["id"])] = str(Path(*reversed(parts)))
    owners = defaultdict(list)
    headers = defaultdict(list)
    for action in data.get("actions", []):
        config = configs.get(str(action.get("configurationId", 0)))
        if config is None:
            raise ValueError("DwyuAnalyzeTarget without configuration")
        target = targets[str(action["targetId"])]
        key = identity(target["label"], config["checksum"])
        outputs = [artifacts[str(file)] for file in action.get("outputIds", [])]
        reports = [name for name in outputs if name.endswith("_dwyu_report.json")]
        if action["mnemonic"] == "DwyuAnalyzeTarget":
            if len(reports) != 1:
                raise ValueError("ambiguous DwyuAnalyzeTarget report: " + repr((key, reports)))
            owners[key].append(reports[0])
        for name in outputs:
            if name.endswith(".dwyu_owned_headers.json"):
                contents = json.loads(action["fileContents"])
                if any(not isinstance(contents.get(field), typ) for field, typ in (
                    ("public", dict), ("private", dict), ("required", list),
                    ("raw_assembly", list), ("unchecked_link_inputs", list),
                )):
                    raise ValueError("invalid DWYU header manifest: " + key)
                headers[key].append({"artifact": name, "contents": contents})
    return dict(owners), dict(headers)


def audit_closure(audit, roots, rows, eligible, manifest):
    """Apply DWYU to roots in their original configurations, then audit each action."""
    expr = "outputs('.*(_dwyu_report[.]json|[.]dwyu_owned_headers[.]json)', deps(" + query_labels(roots) + "))"
    aspect = "--aspects=//bazel/rules:dwyu.bzl%dwyu_closure"
    actions = audit.query("closure-analyzers", "aquery", expr, (
        aspect, "--include_aspects", "--include_artifacts", "--include_file_write_contents",
        "--output_groups=dwyu",
    ))
    owners, headers = configured_analyzers(actions)
    manifest["analyzers_not_in_original_closure"] = sorted(set(owners) - set(rows))
    manifest["ambiguous_analyzers"] = {
        key: value for key, value in owners.items() if len(value) != 1
    }
    info = audit.command("closure-execution-root", "info", ["execution_root"])
    if info["exit_code"]:
        raise RuntimeError("bazel info execution_root failed")
    execroot = Path(Path(info["stdout"]["file"]).read_text().strip())

    # Remove only the declared DWYU outputs so neither stale reports nor
    # different top-level configurations can qualify an owner.
    deleted = {}
    for key, paths in owners.items():
        for relative in paths:
            path = execroot / relative
            relative_output = path.relative_to(execroot / "bazel-out")
            if ".." in relative_output.parts:
                raise ValueError("DWYU output outside bazel-out: " + str(path))
            removed = path.is_file() or path.is_symlink()
            deleted[key] = removed
            if removed:
                path.unlink()
    bep = audit.out / "closure-strict.bep.json"
    native = audit.command("closure-ordinary", "build", [
        "--keep_going", "--remote_download_outputs=minimal",
        "--build_event_json_file=" + str(audit.out / "closure-ordinary.bep.json"),
    ] + roots)
    for row in eligible.values():
        row["ordinary"] = {
            "status": "passed" if native["exit_code"] == 0 else "not_proved",
            "reason": "original configured root build" if native["exit_code"] == 0 else "original root build failed",
        }
    run = audit.command("closure-strict", "build", [
        "--keep_going", aspect, "--output_groups=dwyu",
        "--remote_download_outputs=toplevel", "--build_event_publish_all_actions",
        "--build_event_json_file=" + str(bep),
    ] + roots)
    run["bep"] = sha_file(bep)
    executed = defaultdict(list)
    filesets = {}
    remaining = []
    with bep.open() as stream:
        for line in stream:
            event = json.loads(line)
            if "started" in event:
                run["invocation_uuid"] = event["started"].get("uuid")
            completed = event.get("id", {}).get("actionCompleted")
            action = event.get("action", event.get("actionExecuted", {}))
            if completed and action.get("type") == "DwyuAnalyzeTarget":
                key = identity(completed["label"], completed.get("configuration", {}).get("id", ""))
                executed[key].append((completed.get("primaryOutput"), action))
            if "namedSet" in event.get("id", {}):
                filesets[event["id"]["namedSet"]["id"]] = event["namedSetOfFiles"]
            target = event.get("id", {}).get("targetCompleted", {})
            if canonical(target.get("label", "")) in roots:
                remaining.extend(
                    f["id"] for group in event.get("completed", {}).get("outputGroup", [])
                    if group.get("name") == "dwyu" for f in group.get("fileSets", [])
                )
    named_files = {}
    visited = set()
    while remaining:
        ident = remaining.pop()
        if ident in visited:
            continue
        visited.add(ident)
        entry = filesets[ident]
        remaining.extend(f["id"] for f in entry.get("fileSets", []))
        for item in entry.get("files", []):
            name = str(Path(*item.get("pathPrefix", [])) / item["name"])
            named_files[name] = item
    for key, row in eligible.items():
        paths = owners.get(key, [])
        report = {
            "status": "missing", "previous_report_removed": deleted.get(key, False),
            "invocation_uuid": run.get("invocation_uuid"),
        }
        row["strict"] = report
        if len(paths) != 1:
            report["reason"] = "zero or multiple configured DwyuAnalyzeTarget actions"
            continue
        configured_headers = headers.get(key, [])
        if len(configured_headers) != 1:
            report["reason"] = "zero or multiple configured DWYU header manifests"
            continue
        report["configured_headers"] = configured_headers[0]
        relative = paths[0]
        path = execroot / relative
        actions_for_output = [(name, action) for name, action in executed.get(key, []) if name == relative]
        if not path.is_file():
            report["status"] = "failed" if any(not a.get("success") for _, a in actions_for_output) else "missing"
            report["reason"] = "configured report was not produced"
            continue
        report["report"] = sha_file(path)
        source = json.loads(path.read_bytes())
        if canonical(source.get("analyzed_target", "")) != row["label"] or any(
            not isinstance(source.get(field), typ) for field, typ in FIELDS.items()
        ) or not isinstance(source.get("unverified_compilation_deps"), list) or any(
            not isinstance(reason, str) or not reason for reason in source["unverified_compilation_deps"]
        ):
            report["status"] = "invalid"
            report["reason"] = "invalid original report fields or owner"
            continue
        try:
            validate_link_evidence(source)
        except ValueError as exc:
            report["status"] = "invalid"
            report["reason"] = str(exc)
            continue
        report["findings"] = {field: len(source[field]) for field in FIELDS}
        report["findings"]["unverified_link_deps"] = len(source.get("unverified_link_deps", []))
        candidates = [a["primaryOutput"] for _, a in actions_for_output if a.get("success") and "primaryOutput" in a]
        if relative in named_files:
            candidates.append(named_files[relative])
        proved = any(
            f.get("digest") == report["report"]["sha256"] and
            int(f.get("length", -1)) == report["report"]["bytes"]
            for f in candidates
        )
        if source.get("unchecked_link_inputs") or configured_headers[0]["contents"]["unchecked_link_inputs"]:
            report["status"] = "unchecked_link_inputs"
        elif source["unverified_compilation_deps"]:
            report["status"] = "unverified_compilation_deps"
            report["reason"] = source["unverified_compilation_deps"]
        elif any(report["findings"].values()):
            report["status"] = "findings"
        elif configured_headers[0]["contents"]["raw_assembly"]:
            report["status"] = "unchecked_assembly"
        elif proved:
            report["status"] = "clean"
        else:
            report["status"] = "not_proved"
            report["reason"] = "no matching successful action or original BEP file digest"


def report_path(execroot, row):
    repo, suffix = row["label"].split("//", 1)
    pkg, name = suffix.split(":", 1)
    path = execroot / "bazel-out" / row["mnemonic"] / "bin"
    if repo:
        if not repo.startswith("@@"):
            raise ValueError("query returned apparent repository for " + row["label"])
        path = path / "external" / repo[2:]
    return path / pkg / (name + "_dwyu_report.json")


def select_completions(events, strict):
    selected = []
    for event in events:
        completed = event.get("completed", {})
        groups = [g.get("name") for g in completed.get("outputGroup", [])]
        aspect = event["id"]["targetCompleted"].get("aspect", "")
        if (("dwyu" in aspect.lower()) or ("dwyu" in groups)) == strict:
            selected.append(event)
    return selected


def read_strict(row, events, path, original_deleted, filesets):
    selected = select_completions(events, True)
    result = {"status": "missing", "completions": selected, "previous_report_removed": original_deleted}
    if not selected:
        result["reason"] = "no fresh DWYU completion for label and configuration"
        return result
    if not path.is_file():
        result["status"] = "failed" if any(not e.get("completed", {}).get("success") for e in selected) else "missing"
        result["reason"] = "no report from this selected configuration"
        return result
    result["report"] = sha_file(path)
    try:
        report = json.loads(path.read_bytes())
        if canonical(report.get("analyzed_target", "")) != row["label"]:
            raise ValueError("analyzed_target mismatch")
        if any(not isinstance(report.get(field), typ) for field, typ in FIELDS.items()):
            raise ValueError("missing or invalid DWYU result fields")
        if not isinstance(report.get("unverified_compilation_deps"), list) or any(
            not isinstance(reason, str) or not reason for reason in report["unverified_compilation_deps"]
        ):
            raise ValueError("invalid compiler directory dependency reasons")
        validate_link_evidence(report)
        result["findings"] = {field: len(report[field]) for field in FIELDS}
        result["findings"]["unverified_link_deps"] = len(report.get("unverified_link_deps", []))
        if all(e.get("completed", {}).get("success") for e in selected):
            # Successful report files must also match this BEP's named sets.
            # Failed actions can have fresh report bytes absent from their
            # incomplete named sets, so their config-checked fallback is above.
            remaining = [f["id"] for e in selected for g in e["completed"].get("outputGroup", [])
                         if g.get("name") == "dwyu" for f in g.get("fileSets", [])]
            visited = set()
            matches = []
            while remaining:
                ident = remaining.pop()
                if ident in visited:
                    continue
                visited.add(ident)
                entry = filesets[ident]
                remaining.extend(f["id"] for f in entry.get("fileSets", []))
                for item in entry.get("files", []):
                    relative = Path(*item.get("pathPrefix", [])) / item["name"]
                    if str(path).endswith("/" + str(relative)):
                        matches.append(item)
            if len(matches) != 1 or matches[0].get("digest") != result["report"]["sha256"] or int(
                    matches[0].get("length", -1)) != result["report"]["bytes"]:
                raise ValueError("fresh report lacks matching successful BEP digest/length")
        if report.get("unchecked_link_inputs"):
            result["status"] = "unchecked_link_inputs"
        elif report["unverified_compilation_deps"]:
            result["status"] = "unverified_compilation_deps"
            result["reason"] = report["unverified_compilation_deps"]
        elif any(result["findings"].values()):
            result["status"] = "findings"
        elif not all(e.get("completed", {}).get("success") for e in selected):
            result["status"] = "failed"
            result["reason"] = "zero findings but aspect failed"
        else:
            result["status"] = "clean"
    except (ValueError, AttributeError, KeyError, TypeError) as exc:
        result["status"] = "invalid"
        result["reason"] = str(exc)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, choices=("cmake_match", "cmake_match_aarch64"))
    parser.add_argument("--out", required=True, type=Path, help="new output directory; raw queries stay here")
    parser.add_argument("--seed", action="append", help="explicit binary/standalone/test root, default //:clickhouse")
    parser.add_argument("--audit-target", action="append", default=[],
                        help="audit only these explicitly chosen reachable owners; others remain not_tested")
    parser.add_argument("--enumerate-only", action="store_true")
    args = parser.parse_args()
    if Path(".bazelversion").read_text().strip() != "9.3.0":
        raise SystemExit("This inventory requires the pinned Bazel 9.3.0 analysis_v2 schema")
    audit = Audit(args)
    roots = args.seed or ["//:clickhouse"]
    manifest = {"roots": roots, "scope": "configured closure of explicit roots only, not whole repository",
                "config": args.config, "bazel_version": "9.3.0", "runs": audit.runs,
                "rules": {}, "audit_passed": False}
    try:
        expr = "deps(" + query_labels(roots) + ")"
        cq = audit.query("closure", "cquery", expr)
        rows, skipped = read_cquery(cq)
        manifest["rules"] = rows
        manifest["unconfigured_and_files"] = skipped
        aq = audit.query("actions", "aquery", expr)
        manifest["actions_not_matched_to_configured_rules"] = add_actions(aq, rows)
        eligible = {}
        for key, row in rows.items():
            if row["is_tool"]:
                row["category"] = "tool_configuration"
            elif row["rule_class"] in NATIVE_CC:
                if any(count is None for count in row["sources"].values()):
                    row["category"] = "unresolved_native_sources"
                elif any(row["sources"].values()):
                    if row["actions"].get("CppCompile"):
                        row["category"] = "compiled_native"
                    elif not row["sources"]["srcs"]:
                        row["category"] = "header_only_native"
                    else:
                        row["category"] = "native_with_non_cpp_compile_sources"
                    eligible[key] = row
                else:
                    row["category"] = "native_forwarder_no_sources"
            elif row["rule_class"] in OTHER_CC or any(
                    re.search(r"CppCompile|Assemble|Rustc|RustCompile", action) for action in row["actions"]):
                row["category"] = "unsupported_compiled_or_cc_provider"
            elif row["rule_class"] == "alias":
                row["category"] = "alias"
            else:
                row["category"] = "other_rule_or_generator"
            row["ordinary"] = {"status": "not_tested"}
            row["strict"] = {"status": "not_tested"}
        toplevel_labels = [r["label"] for r in eligible.values()]
        if args.audit_target:
            toplevel_labels = args.audit_target
        if not args.enumerate_only and not args.audit_target:
            audit_closure(audit, roots, rows, eligible, manifest)
        elif not args.enumerate_only and toplevel_labels:
            # Top-level configurations can differ from the same label beneath
            # refs, implementation_deps or another incoming transition.
            top = audit.query("requested", "cquery", "config(" + query_labels(toplevel_labels) + ", target)")
            top_rows, _ = read_cquery(top)
            selected = {}
            for key, row in eligible.items():
                if key in top_rows:
                    selected[key] = row
                elif row["label"] in {r["label"] for r in top_rows.values()}:
                    row["ordinary"] = row["strict"] = {"status": "configuration_not_reproduced",
                        "reason": "This label is not top-level in the configuration required by the root.",
                        "top_level_checksums": sorted({r["checksum"] for r in top_rows.values()
                                                       if r["label"] == row["label"]})}
            manifest["requested_not_in_eligible_closure"] = sorted(set(top_rows) - set(eligible))
            if selected:
                info = audit.command("execution-root", "info", ["execution_root"])
                if info["exit_code"]:
                    raise RuntimeError("bazel info execution_root failed")
                execroot = Path(Path(info["stdout"]["file"]).read_text().strip())
                labels = [row["label"] for row in selected.values()]
                native, native_events, _ = audit.build("ordinary", labels, False)
                for key, row in selected.items():
                    completed = select_completions(native_events.get(key, []), False)
                    status = "missing" if not completed else ("passed" if all(
                        e.get("completed", {}).get("success") for e in completed) else "failed")
                    row["ordinary"] = {"status": status, "invocation_uuid": native.get("invocation_uuid"),
                                       "completions": completed}
                deleted = {}
                for key, row in selected.items():
                    path = report_path(execroot, row)
                    deleted[key] = path.is_file() or path.is_symlink()
                    if deleted[key]:
                        path.unlink()
                strict, strict_events, filesets = audit.build("strict", labels, True)
                for key, row in selected.items():
                    row["strict"] = read_strict(row, strict_events.get(key, []),
                                                report_path(execroot, row), deleted[key], filesets)
                    row["strict"]["invocation_uuid"] = strict.get("invocation_uuid")
                    if not strict_events.get(key):
                        alternatives = [k for k in strict_events if k.startswith(row["label"] + " (")]
                        if alternatives:
                            row["strict"] = {"status": "configuration_not_reproduced",
                                             "actual_strict_identities": alternatives,
                                             "invocation_uuid": strict.get("invocation_uuid")}
        manifest["categories"] = dict(Counter(row["category"] for row in rows.values()))
        manifest["ordinary"] = dict(Counter(row["ordinary"]["status"] for row in eligible.values()))
        manifest["strict"] = dict(Counter(row["strict"]["status"] for row in eligible.values()))
        # Keep every owner of production code or headers in the keyed report.
        # The complete tool and generator rows remain in the original cquery,
        # including their checksums and input edges. This avoids copying the
        # compiler's own entire source graph into each ClickHouse audit result.
        untested_by_class = Counter(
            (row["category"], row["rule_class"]) for row in rows.values()
            if row["category"] in ("tool_configuration", "other_rule_or_generator")
        )
        manifest["other_rules_by_class"] = [
            {"category": category, "rule_class": rule_class, "count": count}
            for (category, rule_class), count in sorted(untested_by_class.items())
        ]
        # Keep each C/C++ tool configuration visible. The original query
        # contains its complete configured_inputs; avoid copying those large
        # tool graphs into the manifest a second time.
        manifest["tool_cc_rules"] = {
            key: {field: row[field] for field in (
                "label", "checksum", "mnemonic", "is_tool", "rule_class",
                "actions", "sources", "ordinary", "strict",
            )}
            for key, row in rows.items()
            if row["is_tool"] and (row["rule_class"] in NATIVE_CC | OTHER_CC or any(
                re.search(r"CppCompile|Assemble|Rustc|RustCompile", action) for action in row["actions"]))
        }
        manifest["tool_ordinary"] = dict(Counter(
            row["ordinary"]["status"] for row in manifest["tool_cc_rules"].values()))
        manifest["tool_strict"] = dict(Counter(
            row["strict"]["status"] for row in manifest["tool_cc_rules"].values()))
        manifest["rules"] = {
            key: row for key, row in rows.items()
            if row["category"] not in ("tool_configuration", "other_rule_or_generator")
            or row["label"] in [canonical(label) for label in roots]
        }
        blockers = ("unresolved_native_sources", "unsupported_compiled_or_cc_provider")
        manifest["audit_passed"] = bool(eligible) and not manifest["tool_cc_rules"] and not manifest.get(
            "ambiguous_analyzers") and not manifest.get("analyzers_not_in_original_closure") and not any(
            row["category"] in blockers for row in rows.values()) and not manifest[
                "actions_not_matched_to_configured_rules"] and not manifest.get(
                    "requested_not_in_eligible_closure") and all(
                        row["ordinary"]["status"] == "passed" and row["strict"]["status"] == "clean"
                        for row in eligible.values())
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        manifest["error"] = type(exc).__name__ + ": " + str(exc)
    path = audit.out / "manifest.json"
    write_json(path, manifest)
    print("FULL_DWYU_MANIFEST=" + json.dumps({
        **sha_file(path), "audit_passed": manifest["audit_passed"],
        "categories": manifest.get("categories"), "ordinary": manifest.get("ordinary"),
        "strict": manifest.get("strict"), "error": manifest.get("error")
    }, sort_keys=True), flush=True)
    return 0 if manifest["audit_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Audit the configured C/C++ owners reachable from explicit Bazel roots.

Run outside Bazel, from the workspace root. Raw queries and BEPs stay in
--out; manifest.json is keyed by (canonical label, configuration checksum).
"""

import argparse
import base64
from collections import Counter, defaultdict
import hashlib
import gzip
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

DECLARED_CC = NATIVE_CC | {
    "ch_cc_library", "ch_cc_binary", "ch_cc_test",
    "poco_cc_library", "ch_grpc_cc_library", "llvm_cc_library", "zxc_cc_library", "parquet_cc_binary",
}
FIXTURE_PACKAGE = "bazel/initial_migration/dwyu_regressions/"
DWYU_ASPECT = "//bazel/rules:dwyu.bzl%"
DWYU_ASPECTS = {"dwyu", "dwyu_closure", "dwyu_production_closure"}
DWYU_PARAMETERS = {
    "dwyu_analysis_ignores_private_headers_from_deps": "true",
    "dwyu_analysis_optimizes_impl_deps": "true",
    "dwyu_analysis_reports_missing_direct_deps": "true",
    "dwyu_analysis_reports_unused_deps": "true",
    "dwyu_verbose": "false",
}


def read_production_census(path, digest, workspace=Path(".")):
    """Validate an externally prepared census against the BUILD files under audit."""
    raw = path.read_bytes()
    if not re.fullmatch(r"[0-9a-f]{64}", digest or "") or hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("production census SHA-256 mismatch")
    census = json.loads(raw)
    if (set(census) != {"schema", "revision", "tree", "inventory_sha256", "build_files", "declarations"}
            or census["schema"] != 1
            or not re.fullmatch(r"[0-9a-f]{40}", census["revision"])
            or not re.fullmatch(r"[0-9a-f]{40}", census["tree"])
            or not re.fullmatch(r"[0-9a-f]{64}", census["inventory_sha256"])
            or not isinstance(census["build_files"], dict) or not census["build_files"]
            or not isinstance(census["declarations"], dict) or not census["declarations"]):
        raise ValueError("invalid production census")
    repositories = set()
    for name, record in census["build_files"].items():
        relative = Path(name)
        if (relative.is_absolute() or ".." in relative.parts or
                not (name.endswith("/BUILD.bazel") or name == "BUILD.bazel" or name.endswith(".BUILD.bazel")) or
                set(record) != {"sha1", "owner", "repo", "package", "declarations"} or
                record["owner"] not in ("workspace", "contrib", "other-overlay")):
            raise ValueError("invalid census BUILD file: " + name)
        body = (workspace / relative).read_bytes()
        git_blob = hashlib.sha1(b"blob " + str(len(body)).encode() + b"\0" + body).hexdigest()
        if git_blob != record["sha1"]:
            raise ValueError("census BUILD file changed: " + name)
        if record["owner"] == "workspace":
            if record["repo"] != "":
                raise ValueError("workspace BUILD claims an external repository: " + name)
            repositories.add("//")
        elif record["owner"] == "contrib":
            repo = record["repo"]
            if (not isinstance(repo, str) or not re.fullmatch(r"@clickhouse_[a-z0-9_]+", repo)
                    or name != "bazel/dependencies/" + repo[1:] + ".BUILD.bazel"):
                raise ValueError("contrib repository differs from BUILD metadata: " + name)
            repositories.add(census_canonical(repo + "//"))
    production, fixtures = {}, {}
    for label, decl in census["declarations"].items():
        query_labels([label])
        if (set(decl) != {"label", "file", "kind", "line", "owner"} or decl["label"] != label or
                decl["kind"] not in DECLARED_CC or type(decl["line"]) is not int or decl["line"] < 1):
            raise ValueError("invalid census declaration: " + label)
        build = census["build_files"].get(decl["file"])
        if not build or decl["owner"] != build["owner"]:
            raise ValueError("census declaration lacks its BUILD file: " + label)
        prefix = build["repo"] + "//" + build["package"] + ":"
        if not label.startswith(prefix):
            raise ValueError("declaration disagrees with BUILD package: " + label)
        if decl["file"].startswith(FIXTURE_PACKAGE):
            package = build["package"]
            if (decl["owner"] != "workspace" or
                    not (package == FIXTURE_PACKAGE.rstrip("/") or package.startswith(FIXTURE_PACKAGE))):
                raise ValueError("non-fixture declaration claimed as fixture: " + label)
            fixtures[label] = decl
        elif decl["owner"] in ("workspace", "contrib"):
            production[label] = decl
    if not production:
        raise ValueError("production census has no owned C/C++ declarations")
    if "//" not in repositories or any(
            label_repository(census_canonical(label)) not in repositories for label in production):
        raise ValueError("production declarations are outside their verified repositories")
    return {
        "source": sha_file(path), "revision": census["revision"], "tree": census["tree"],
        "inventory_sha256": census["inventory_sha256"], "production": production,
        "owned_repositories": sorted(repositories),
        "fixture_declarations_not_scored": fixtures,
        "ignored_declarations": dict(Counter(d["kind"] for d in census["declarations"].values()
                                             if d["owner"] == "other-overlay")),
        "fixture_expectations": "not tested by production closure; test controls separately",
    }


def census_canonical(label):
    """Bzlmod's checked-in contrib module creates these repository names."""
    if label.startswith("@clickhouse_") and "//" in label:
        return "@@+contrib+" + label[1:]
    return canonical(label)


def label_repository(label):
    """Return the exact repository of a --consistent_labels query result."""
    repo, separator, _ = canonical(label).partition("//")
    if not separator or (repo and not re.fullmatch(r"@@[a-zA-Z0-9_.+~-]+", repo)):
        raise ValueError("query did not return a canonical repository: " + label)
    return repo + "//"


def read_compatibility(path, rows, header_labels=()):
    """Bind a Starlark provider observation to a unique config(expr, target) row."""
    by_label = defaultdict(list)
    for key, row in rows.items():
        by_label[row["label"]].append(key)
    seen = {}
    for line in path.read_text().splitlines():
        event = json.loads(line)
        extra = isinstance(event, dict) and "standalone_headers" in event
        headers = event.pop("standalone_headers") if extra else None
        if (not isinstance(event, dict) or set(event) != {"label", "incompatible"} or
                not isinstance(event["label"], str) or type(event["incompatible"]) is not bool):
            raise ValueError("invalid cquery platform provider observation")
        label = canonical(event["label"])
        if len(by_label.get(label, [])) != 1:
            raise ValueError("platform provider has no unique top-level configuration: " + label)
        key = by_label[label][0]
        if key in seen:
            raise ValueError("duplicate platform provider observation: " + key)
        if label in header_labels and not event["incompatible"]:
            if not isinstance(headers, dict):
                raise ValueError("missing requested CcInfo header observation: " + key)
            rows[key]["standalone_headers"] = headers
        elif extra:
            raise ValueError("unexpected CcInfo header observation: " + key)
        seen[key] = event["incompatible"]
    if set(seen) != set(rows):
        raise ValueError("missing platform provider observations: " + repr(sorted(set(rows) - set(seen))))
    return seen


def choose_production_roots(audit, census, initial):
    first = audit.query("initial-closure", "cquery", "deps(" + query_labels(initial) + ")")
    reachable, skipped = read_cquery(first)
    known = {row["label"] for row in reachable.values() if not row["is_tool"]}
    candidates = [label for label in census["production"] if census_canonical(label) not in known]
    result = {"initial": initial, "initial_skipped": skipped, "candidate_count": len(candidates),
              "incompatible": {}, "added_roots": []}
    if not candidates:
        return initial, result
    expr = "config(" + query_labels(candidates) + ", target)"
    top = audit.query("standalone-candidates", "cquery", expr)
    top_rows, omitted = read_cquery(top)
    result["omitted_candidates"] = omitted
    result["candidate_queries"] = {"configured": sha_file(top)}
    expected = {census_canonical(label) for label in candidates}
    observed = {row["label"] for row in top_rows.values() if not row["is_tool"]}
    if expected != observed or omitted:
        raise ValueError("standalone configured census differs from requested declarations: " +
                         repr({"missing": sorted(expected - observed), "unexpected": sorted(observed - expected),
                               "omitted": omitted}))
    compatible, observation = query_compatibility(audit, "standalone-platform", expr, top_rows)
    result["candidate_queries"]["platform"] = observation
    apparent = {census_canonical(label): label for label in candidates}
    for key, incompatible in compatible.items():
        row = top_rows[key]
        if incompatible:
            result["incompatible"][key] = {
                "label": row["label"], "checksum": row["checksum"],
                "evidence": "IncompatiblePlatformProvider in matched cquery target configuration",
            }
        else:
            result["added_roots"].append(apparent[row["label"]])
    return initial + sorted(result["added_roots"]), result


def query_compatibility(audit, name, expr, rows, header_labels=()):
    query_file = audit.out / (name + ".query")
    query_file.write_text(expr + "\n")
    provider_file = audit.out / (name + ".cquery")
    if header_labels:
        from audit_public_headers import provider_query
        provider = provider_query(header_labels)
    else:
        provider = (
            'def format(target):\n'
            '    p = providers(target)\n'
            '    return json.encode({"label": str(target.label), '
            '"incompatible": None if p == None else "IncompatiblePlatformProvider" in p})\n'
        )
    provider_file.write_text(provider)
    run = audit.command(name, "cquery", [
        "--query_file=" + str(query_file), "--output=starlark", "--consistent_labels",
        "--starlark:file=" + str(provider_file),
    ], output=True)
    if run["exit_code"]:
        raise RuntimeError("cquery standalone platform providers failed: " + run["log"]["file"])
    return read_compatibility(Path(run["stdout"]["file"]), rows, header_labels), run["stdout"]


def ordinary_reachability(bep, rows, roots):
    """Only a successful ordinary BEP root grants ordinary coverage to its configured deps."""
    requested = {census_canonical(label) for label in roots}
    success = set()
    invocations = []
    with bep.open() as stream:
        for line in stream:
            event = json.loads(line)
            if "started" in event:
                invocations.append(event["started"].get("uuid"))
            target = event.get("id", {}).get("targetCompleted")
            if not target or target.get("aspect") or canonical(target.get("label", "")) not in requested:
                continue
            key = identity(target["label"], target.get("configuration", {}).get("id", ""))
            if key not in rows:
                raise ValueError("ordinary completed root outside configured closure: " + key)
            groups = event.get("completed", {}).get("outputGroup", [])
            if any(g.get("name") == "dwyu" for g in groups):
                raise ValueError("DWYU completion in ordinary root coverage")
            if event.get("completed", {}).get("success") is True:
                success.add(key)
    if len(invocations) != 1 or not invocations[0]:
        raise ValueError("ordinary BEP requires one invocation")
    reachable = set()
    remaining = list(success)
    while remaining:
        key = remaining.pop()
        if key in reachable:
            continue
        reachable.add(key)
        for dep in rows[key]["configured_inputs"]:
            checksum = dep.get("configurationChecksum")
            if not checksum:  # Source files and unconfigured rules carry no configured C++ proof.
                continue
            child = identity(dep["label"], checksum)
            if child in rows:
                remaining.append(child)
    return {"invocation_uuid": invocations[0], "successful_roots": sorted(success),
            "reachable": reachable, "bep": sha_file(bep)}


def sha_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return {"file": str(path), "sha256": digest.hexdigest(), "bytes": path.stat().st_size}


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def bep_file_matches(reference, original, *, local_path=None):
    """Match BEP metadata against the SHA-256 and size of the original file."""
    if not isinstance(reference, dict):
        return False
    has_metadata = "digest" in reference or "length" in reference
    if has_metadata and (
        reference.get("digest") != original["sha256"] or
        type(reference.get("length")) not in (str, int) or
        str(reference["length"]) != str(original["bytes"])
    ):
        return False
    if "uri" not in reference:
        return has_metadata
    uri = reference["uri"]
    if not isinstance(uri, str):
        return False
    if local_path is not None and uri.startswith("file:"):
        return has_metadata and uri == local_path.absolute().as_uri()
    match = re.fullmatch(
        r"bytestream://[A-Za-z0-9][A-Za-z0-9.-]*(?::[0-9]+)?"
        r"(?:/[A-Za-z0-9][A-Za-z0-9._-]*)*/blobs/([0-9a-f]{64})/(0|[1-9][0-9]*)",
        uri,
    )
    return bool(match and match[1] == original["sha256"] and match[2] == str(original["bytes"]))


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


    holds = report.get("unverified_link_deps", [])
    details = report.get("unverified_link_reasons")
    if (not isinstance(details, dict) or set(details) != set(holds)
            or len(holds) != len(set(holds))):
        raise ValueError("missing or inconsistent DWYU link hold reasons")
    nonopaque = {"alwayslink", "static_archive", "unsupported_nm_kind"}
    symbol_causes = {
        "opaque_input", "archive_veto", "alwayslink_provider", "weak_undefined",
        "multiple_definitions", "multiple_routes", "duplicate_resolved_route", "non_strong_definition",
    }
    for dep, entry in details.items():
        if (not isinstance(entry, dict) or set(entry) != {
            "forwarded_inputs", "source_free_forwarder", "input_reasons",
            "opaque_input_reasons", "ambiguous_symbols", "raw_link_candidate",
        } or any(type(entry[key]) is not bool for key in
                 ("forwarded_inputs", "source_free_forwarder", "raw_link_candidate"))):
            raise ValueError("invalid DWYU link hold reason")
        for key in ("input_reasons", "opaque_input_reasons"):
            indices = entry[key]
            if (not isinstance(indices, list) or any(type(index) is not int or
                    not 0 <= index < len(reasons) for index in indices) or len(set(indices)) != len(indices)):
                raise ValueError("invalid DWYU link hold input indices")
            for index in indices:
                reason = reasons[index]
                if key == "input_reasons" and not any(route["target"] == dep for route in reason["routes"]):
                    raise ValueError("DWYU link hold cites another dependency's input")
                if key == "opaque_input_reasons" and reason["reason"] in nonopaque:
                    raise ValueError("DWYU opaque link hold cites a scannable input")
        symbols = entry["ambiguous_symbols"]
        if not isinstance(symbols, dict):
            raise ValueError("invalid DWYU ambiguous link symbols")
        for symbol, causes in symbols.items():
            if (not isinstance(symbol, str) or not symbol or not isinstance(causes, list) or not causes or any(
                not isinstance(cause, str) or cause not in symbol_causes for cause in causes
            ) or len(set(causes)) != len(causes) or
                    ("duplicate_resolved_route" in causes and "multiple_routes" not in causes)):
                raise ValueError("invalid DWYU ambiguous link symbol causes")
        if not any(entry.values()):
            raise ValueError("unexplained DWYU link hold")


def validate_compiler_coverage(report):
    """A required file which Clang never entered prevents a whole-target pass."""
    missing = report.get("unentered_owned_files")
    reserved = report.get("reserved_compilation_deps")
    if not isinstance(missing, dict) or any(
        not isinstance(file, str) or not file or visibility not in ("public", "private")
        for file, visibility in missing.items()
    ):
        raise ValueError("invalid unentered DWYU owned files")
    if not isinstance(reserved, dict) or set(reserved) != {"deps", "implementation_deps"}:
        raise ValueError("invalid DWYU reserved compilation dependencies")
    for deps in reserved.values():
        if (not isinstance(deps, list) or any(not isinstance(dep, str) or not dep for dep in deps)
                or len(set(deps)) != len(deps)):
            raise ValueError("invalid DWYU reserved compilation dependency labels")
    if not missing and any(reserved.values()):
        raise ValueError("DWYU reserves dependencies with complete compiler coverage")
    if missing and any(report[field] for field in
                       ("unused_deps", "unused_implementation_deps", "deps_which_should_be_private")):
        raise ValueError("DWYU suggests changing dependencies with incomplete compiler coverage")


def validate_runtime_compiler_inputs(report, configured_headers):
    """The producer's legacy inputs are not public headers or implicit owners."""
    unknown = report.get("unverified_compiler_inputs")
    declared = configured_headers.get("unverified_compiler_inputs")
    if (not isinstance(unknown, list) or not isinstance(declared, list)
            or any(not isinstance(file, str) or not file for file in unknown + declared)
            or len(set(unknown)) != len(unknown) or len(set(declared)) != len(declared)
            or set(unknown) != set(declared)):
        raise ValueError("invalid or inconsistent runtime unverified compiler inputs")
    return unknown


def runtime_cpp_candidate(row):
    """Select only configured C/C++ actions; an ASM/empty producer has no CPP report."""
    return (row["rule_class"] == "llvm_runtime_objects"
            and row.get("language") in ("C", "CXX")
            and row["sources"]["srcs"] is not None and row["sources"]["srcs"] > 0
            and row["actions"].get("CppCompile", 0) > 0
            and not any(row["actions"].get(name, 0) for name in (
                "Assemble", "CppCompileHeader", "CppModuleCompile")))


def runtime_asm_candidate(row):
    """Only original Assemble actions can propose preprocessed runtime ASM."""
    return (row["rule_class"] == "llvm_runtime_objects"
            and row.get("language") == "ASM"
            and row["sources"]["srcs"] is not None and row["sources"]["srcs"] > 0
            and row["actions"].get("Assemble", 0) > 0
            and not any(row["actions"].get(name, 0) for name in (
                "CppCompile", "CppCompileHeader", "CppModuleCompile")))


def runtime_compile_candidate(row):
    return runtime_cpp_candidate(row) or runtime_asm_candidate(row)


def runtime_action_name(row):
    return "Assemble" if row["language"] == "ASM" else "CppCompile"


def configuration_map(data, field):
    return {str(config["id"]): config for config in data.get(field, [])}


def read_cquery(path):
    return parse_cquery(path.read_bytes())


def parse_cquery(raw):
    data = json.loads(raw)
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
            "language": (attrs.get("language", {}).get("stringValue")
                         if "selectorList" not in attrs.get("language", {}) else None),
            "tags": attrs.get("tags", {}).get("stringListValue", []),
            "features": attrs.get("features", {}).get("stringListValue", []),
            "actions": {},
            "configured_inputs": rule.get("configuredRuleInput", []),
            "dependency_attributes": {
                name: attrs[name] for name in ("deps", "implementation_deps", "refs", "actual",
                                               "exports", "option_deps", "cc_deps", "cc_implementation_deps")
                if name in attrs
            },
        }
        if "additional_compiler_inputs" in attrs:
            rule_inputs = attrs["additional_compiler_inputs"]
            rules[key]["extra_compiler_inputs"] = (None if "selectorList" in rule_inputs
                                                  else len(rule_inputs.get("stringListValue", [])))
    return rules, dict(sorted(skipped.items()))


def read_action_graph(graph):
    return graph if isinstance(graph, dict) else json.loads(graph.read_bytes())


def declared_action_graph(path, rows, name, accounting):
    """Join Bazel 9.3 aquery actions to config(set(...), target) from cquery."""
    data = json.loads(path.read_bytes())
    requested = {r["label"]: r for r in rows.values()}
    def table(field):
        entries = data.get(field, [])
        by_id = {str(entry["id"]): entry for entry in entries}
        if len(by_id) != len(entries):
            raise ValueError("duplicate aquery IDs in " + field)
        return by_id
    targets, configs, classes = map(table, ("targets", "configuration", "ruleClasses"))
    kept, matched, other = [], set(), Counter()
    for action in data.get("actions", []):
        target = targets.get(str(action.get("targetId")), {})
        cfg = configs.get(str(action.get("configurationId")), {})
        label = canonical(target.get("label", ""))
        checksum = cfg.get("checksum", "")
        tool = cfg.get("isTool", False) if cfg else None
        kind = classes.get(str(target.get("ruleClassId")), {}).get("name", "")
        mnemonic = action.get("mnemonic", "")
        row = requested.get(label)
        if (row and kind == row["rule_class"] and type(tool) is bool and
                re.fullmatch("[0-9a-f]{64}", checksum) and mnemonic):
            if checksum == row["checksum"] and tool == row["is_tool"]:
                kept.append(action)
                matched.add((label, mnemonic))
                continue
            if tool and not row["is_tool"] and checksum != row["checksum"]:
                other[("exec", label, checksum, tool, kind, mnemonic)] += 1
                continue
        other[("unmatched", label, checksum, tool, kind, mnemonic)] += 1
    # An exec instance never substitutes for the selected target's own action.
    for group in list(other):
        disposition, label, checksum, tool, kind, mnemonic = group
        if disposition == "exec" and (label, mnemonic) not in matched:
            other[("unmatched", label, checksum, tool, kind, mnemonic)] += other.pop(group)
    groups = [[*group, n] for group, n in sorted(other.items(), key=lambda pair: repr(pair[0]))]
    unmatched = sum(g[-1] for g in groups if g[0] == "unmatched")
    receipt = {"name": name, "input": sha_file(path), "target_actions": len(kept),
               "exec_actions": sum(other.values()) - unmatched, "unmatched_actions": unmatched,
               "total_groups": len(groups), "groups": groups[:64], "groups_sha256": hashlib.sha256(
                   json.dumps(groups, separators=(",", ":")).encode()).hexdigest()}
    accounting[name] = receipt
    print("DWYU_QUERY_OWNER_CONFIGS=" + json.dumps(receipt, separators=(",", ":")), flush=True)
    if unmatched:
        raise ValueError(name + ": actions outside the selected target and its verified exec configuration")
    data["actions"] = kept
    return data


def add_actions(path, rules):
    data = read_action_graph(path)
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
    if any(not isinstance(label, str) or not re.fullmatch(
            r"(@{1,2}[a-zA-Z0-9_.+~-]+)?//[a-zA-Z0-9_./+~=-]*(?::[a-zA-Z0-9_./+~=-]+)?", label
    ) or label.endswith("//") or "..." in label.split("//", 1)[1] for label in labels):
        raise ValueError("not an explicit Bazel label: " + repr(labels))
    # Bazel's query lexer treats '+' in an apparent label as an operator.
    return "set(" + " ".join('"' + label + '"' for label in labels) + ")"


def failed_query_details(log, extra):
    """Retain a bounded, redacted query diagnostic and the original file hashes."""
    queries = [Path(arg[len("--query_file="):]) for arg in extra
               if arg.startswith("--query_file=")]
    if len(queries) != 1:
        raise ValueError("query command needs exactly one query file")
    original = log.read_bytes()
    text = original.decode("utf-8", errors="replace")
    invocation_ids = re.findall(
        r"(?im)^INFO:\s+Invocation ID:\s*([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})\s*$", text)
    # Redact complete lines before truncation: a tail that starts partway
    # through a credential must not contain even the end of its value.
    text = re.sub(
        r"(?im)(\b(?:x-buildbuddy-api-key|authorization|buildbuddy_api_key)\b\s*[:=]\s*)[^\r\n]*",
        r"\1[REDACTED]", text)
    text = re.sub(r"(?im)(--remote(?:_cache|_exec)?_header(?:=|\s+))[^\r\n]*",
                  r"\1[REDACTED]", text)
    redacted = text.encode()
    limit = 8192
    head = redacted[:limit].decode("utf-8", errors="ignore")
    tail = redacted[-limit:].decode("utf-8", errors="ignore") if len(redacted) > limit else ""
    if len(redacted) <= limit * 2:
        tail = redacted[limit:].decode("utf-8", errors="ignore")
    return {"query_file": sha_file(queries[0]),
            "original_stderr": sha_file(log),
            "invocation_ids": invocation_ids,
            "stderr_head": head, "stderr_tail": tail,
            "redacted_bytes_omitted": max(0, len(redacted) - limit * 2)}


class Audit:
    def __init__(self, args):
        self.args = args
        self.production_census = bool(getattr(args, "production_census", None))
        self.out = args.out.resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        self.bazel = ["bazel", "--host_jvm_args=-Xmx20g"]
        self.options = ["--config=" + args.config, "--noannounce_rc", "--color=no",
                        "--curses=no", "--show_progress_rate_limit=30"]
        self.runs = []

    def command(self, name, subcommand, extra, output=False):
        stdout = self.out / (name + (".json" if output else ".stdout"))
        log = self.out / (name + ".log")
        if getattr(self.args, "declared_owners", False) and name == "textual-consumer" and subcommand == "build":
            extra = [v for v in extra if not v.startswith("--remote_download_outputs=")]
            extra += ["--remote_download_outputs=minimal",
                      "--remote_download_regex=.*([.]consumer_[^/]+_(public|private)|_dwyu_consumer_report)[.]json$"]
        command = self.bazel + [subcommand] + self.options + extra
        if getattr(self.args, "declared_owners", False) and subcommand in ("cquery", "aquery"):
            command.append("--bes_backend=")
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
        if rc and subcommand in ("cquery", "aquery"):
            run["failed_query"] = failed_query_details(log, extra)
        self.runs.append(run)
        print("FULL_DWYU_RUN=" + json.dumps(run, sort_keys=True), flush=True)
        return run

    def query(self, name, subcommand, expr, extra=()):
        if name == "requested-cpp-outputs" and getattr(self, "standalone_header_outputs", ()):
            from audit_public_headers import add_outputs
            expr = add_outputs(expr, self.standalone_header_outputs)
        query = self.out / (name + ".query")
        query.write_text(expr + "\n")
        opts = ["--query_file=" + str(query), "--output=jsonproto", "--consistent_labels"]
        if subcommand == "cquery":
            opts += ["--proto:include_configurations",
                     "--transitions=lite", "--noproto:locations",
                     "--noproto:rule_inputs_and_outputs",
                     "--proto:output_rule_attrs=srcs,hdrs,textual_hdrs,tags,features,deps,implementation_deps,"
                     "refs,actual,language,exports,option_deps,cc_deps,cc_implementation_deps"]
            if name == "requested" and getattr(self.args, "standalone_header_owner", ()):
                opts[-1] += ",additional_compiler_inputs"
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
        if strict and getattr(self.args, "declared_owners", False):
            extra += ["--config=dwyu", "--remote_download_outputs=minimal",
                      "--remote_download_regex=.*_dwyu_report[.]json$"]
            if self.args.declared_contexts:
                extra.append("--remote_download_regex=.*_rust_import_evidence[.]json$")
        elif strict:
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
        declared = getattr(self.args, "declared_owners", False)
        started, finished, failed_actions = [], [], defaultdict(list)
        with bep.open() as file:
            for line in file:
                event = json.loads(line)
                if "started" in event:
                    run["invocation_uuid"] = event["started"].get("uuid")
                    started.append(run["invocation_uuid"])
                if declared and "finished" in event:
                    finished.append(event["finished"].get("exitCode", {}).get("code", 0))
                action_id = event.get("id", {}).get("actionCompleted")
                action = event.get("action", {})
                if declared and action_id and action and not action.get("success", False):
                    key = identity(action_id.get("label", ""), action_id.get("configuration", {}).get("id", ""))
                    failed_actions[key].append({"output": action_id.get("primaryOutput"), "type": action.get("type"),
                                                "exit_code": action.get("exitCode")})
                if "namedSet" in event.get("id", {}):
                    filesets[event["id"]["namedSet"]["id"]] = event["namedSetOfFiles"]
                completed = event.get("id", {}).get("targetCompleted")
                if completed:
                    key = identity(completed["label"], completed.get("configuration", {}).get("id", ""))
                    events[key].append(event)
        if declared:
            if (len(started) != 1 or not re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}",
                                                      started[0] or "") or finished != [run["exit_code"]]):
                raise ValueError("missing or contradictory original build start/finish: " + name)
            run["targets"] = sha_file(patterns)
            run["failed_actions"] = dict(failed_actions)
        return run, events, filesets


def configured_analyzers(path):
    """Bind each configured analyzer to its own --owned_headers FileWrite."""
    graph = RuntimeActionGraph(path, ("DwyuAnalyzeTarget", "FileWrite"), None, rule_classes=None)
    by_path = defaultdict(list)
    for ident, artifact in graph.artifacts.items():
        by_path[artifact].append(ident)
    writes = defaultdict(list)
    for key, actions in graph.owners.items():
        for action in actions:
            if action["mnemonic"] == "FileWrite":
                for ident in action.get("outputIds", []):
                    writes[str(ident)].append((key, action))
    owners = defaultdict(list)
    headers = defaultdict(list)
    for key, actions in graph.owners.items():
        for action in actions:
            if action["mnemonic"] != "DwyuAnalyzeTarget":
                continue
            outputs = graph.files(action, "outputIds")
            if len(outputs) != 1 or not outputs[0].endswith("_dwyu_report.json"):
                raise ValueError("ambiguous DwyuAnalyzeTarget report: " + repr((key, outputs)))
            argv = analyzer_arguments(action)
            report = analyzer_option(argv, "--output")
            owned = analyzer_option(argv, "--owned_headers")
            inspected = analyzer_option(argv, "--target_under_inspection")
            if (identity(inspected, graph.configs[str(action["configurationId"])]["checksum"]) != key
                    or report != outputs[0] or len(by_path[report]) != 1
                    or len(by_path.get(owned, [])) != 1 or owned in graph.tree_artifacts
                    or report in graph.tree_artifacts or not owned.endswith(".dwyu_owned_headers.json")):
                raise ValueError("wrong configured analyzer owner, report or header artifact: " + key)
            ident = by_path[owned][0]
            found = writes.get(ident, [])
            if (len(found) != 1 or found[0][0] != key
                    or [str(i) for i in found[0][1].get("outputIds", [])] != [ident]
                    or ident not in graph.input_ids(action)):
                raise ValueError("analyzer --owned_headers has no unique same-owner FileWrite input: " + key)
            contents = json.loads(found[0][1]["fileContents"])
            if any(not isinstance(contents.get(field), typ) for field, typ in (
                ("public", dict), ("private", dict), ("required", list),
                ("raw_assembly", list), ("unchecked_link_inputs", list),
            )):
                raise ValueError("invalid DWYU header manifest: " + key)
            if owners[key]:
                raise ValueError("ambiguous DwyuAnalyzeTarget report: " + repr((key, owners[key] + [report])))
            owners[key].append(report)
            headers[key].append({"artifact": owned, "contents": contents})
    return dict(owners), dict(headers)


def analyzer_arguments(action):
    """Expand only param files recorded by this original Bazel action."""
    args = action.get("arguments")
    if not isinstance(args, list) or any(not isinstance(arg, str) for arg in args):
        raise ValueError("configured analyzer has no command-line arguments")
    parameters = {}
    for item in action.get("paramFiles", []):
        name = item["execPath"]
        content = item["arguments"]
        if (not isinstance(name, str) or not name or name in parameters
                or Path(name).is_absolute() or ".." in Path(name).parts
                or not isinstance(content, list) or any(not isinstance(arg, str) for arg in content)):
            raise ValueError("invalid configured analyzer param file")
        parameters[name] = content
    expanded, used = [], set()
    for arg in args:
        if arg.startswith("@") and arg[1:] in parameters:
            name = arg[1:]
            if name in used:
                raise ValueError("repeated configured analyzer param file")
            used.add(name)
            expanded.extend(parameters[name])
        else:
            expanded.append(arg)
    if used != set(parameters):
        raise ValueError("unbound configured analyzer param file")
    return expanded


def analyzer_option(args, name):
    """DWYU make_param_file_args uses --name followed by its values."""
    if args.count(name) != 1:
        raise ValueError("missing or repeated configured analyzer option " + name)
    pos = args.index(name) + 1
    end = pos
    while end < len(args) and not args[end].startswith("--"):
        end += 1
    if end - pos != 1:
        raise ValueError("wrong arity for configured analyzer option " + name)
    return args[pos]


def native_category(key, row, owners=None, headers=None):
    """Classify native files; a header category never proves PP or CppCompile."""
    if row["is_tool"]:
        return "tool_configuration"
    if row["rule_class"] not in NATIVE_CC:
        return "unsupported_rule"
    counts = row["sources"]
    if any(count is None for count in counts.values()):
        return "unresolved_native_sources"
    if not any(counts.values()):
        return "native_forwarder_no_sources"
    if row["actions"].get("CppCompile"):
        return "compiled_native"
    if any(row["actions"].get(name) for name in ("Assemble", "CppModuleCompile", "CppModuleCodegen")):
        return "native_with_non_cpp_compile_sources"
    if (key != identity(row["label"], row["checksum"]) or not isinstance(owners, dict)
            or not isinstance(headers, dict) or len(owners.get(key, [])) != 1
            or len(headers.get(key, [])) != 1):
        return "unresolved_native_sources"
    # _compiler_srcs exhaustively classifies ctx.rule.files.srcs, rejecting
    # unknown files/trees; _compiler_header_manifest records private headers,
    # compile sources, raw assembly and unchecked link inputs. A .h-named
    # generator producing a .o/.a/.S/.cpp file cannot pass these checks.
    manifest = headers[key][0]
    if not isinstance(manifest, dict):
        return "unresolved_native_sources"
    owned = manifest.get("contents")
    if (not isinstance(owned, dict) or not isinstance(manifest.get("artifact"), str)
            or not manifest["artifact"].endswith(".dwyu_owned_headers.json") or any(
                not isinstance(owned.get(field), typ) for field, typ in (
                    ("public", dict), ("private", dict), ("required", list),
                    ("raw_assembly", list), ("unchecked_link_inputs", list)))):
        return "unresolved_native_sources"
    if any(owned.get(field) for field in (
            "raw_assembly", "unchecked_link_inputs", "raw_link_candidates", "unverified_compiler_inputs")):
        return "native_with_non_cpp_compile_sources"
    public, private, required = owned["public"], owned["private"], owned["required"]
    paths = list(public) + list(public.values()) + list(private) + list(private.values()) + required
    if not paths or any(not isinstance(path, str) or not path or Path(path).is_absolute()
                        or ".." in Path(path).parts for path in paths):
        return "unresolved_native_sources"
    if not set(required).issubset(set(public.values()) | set(private.values())):
        return "unresolved_native_sources"
    # Private source files are in "private" but not in "required". A file
    # explicitly present in textual_hdrs as well as srcs must still have an
    # actual header suffix. Do not infer files from a configured label's name.
    suffixes = {".h", ".hh", ".hpp", ".hxx", ".h++", ".H", ".inc", ".inl", ".ipp", ".tcc", ".txx"}
    source_public_headers = (
        counts["srcs"] == 0
        and bool(counts["hdrs"] or counts["textual_hdrs"])
        and not private
        and not row["actions"]
        and all(not path.startswith("bazel-out/") for path in public.values())
    )
    if (not set(private.values()).issubset(set(required))
            or any(Path(path).suffix not in suffixes
                   and not (source_public_headers and not Path(path).suffix)
                   for path in paths)):
        return "native_with_non_cpp_compile_sources"
    if counts["srcs"] and not private:
        # Nonempty srcs labels may expand to no files. No assertion about
        # private headers can be made without a private File.
        return "unresolved_native_sources"
    return "header_only_native"


class RuntimeActionGraph:
    """Decode original aquery artifacts and actions, optionally constrained to cquery rows."""

    def __init__(self, path, mnemonic, rows, rule_classes=("llvm_runtime_objects",)):
        data = read_action_graph(path)
        self.configs = configuration_map(data, "configuration")
        self.aspect_descriptors = {str(a["id"]): a for a in data.get("aspectDescriptors", [])}
        self.targets = {str(t["id"]): t for t in data.get("targets", [])}
        classes = {str(c["id"]): c["name"] for c in data.get("ruleClasses", [])}
        fragments = {str(f["id"]): f for f in data.get("pathFragments", [])}
        self.artifacts = {}
        self.tree_artifacts = set()
        for artifact in data.get("artifacts", []):
            parts, seen = [], set()
            current = str(artifact["pathFragmentId"])
            while current != "0":
                if current in seen or current not in fragments:
                    raise ValueError("invalid runtime aquery artifact parent")
                seen.add(current)
                fragment = fragments[current]
                parts.append(fragment["label"])
                current = str(fragment.get("parentId", 0))
            path = str(Path(*reversed(parts)))
            if path == "." or Path(path).is_absolute() or ".." in Path(path).parts:
                raise ValueError("invalid runtime aquery artifact: " + path)
            ident = str(artifact["id"])
            if ident in self.artifacts:
                raise ValueError("duplicate runtime aquery artifact ID: " + ident)
            self.artifacts[ident] = path
            if artifact.get("isTreeArtifact", False):
                self.tree_artifacts.add(path)
        self.input_sets = {str(s["id"]): s for s in data.get("depSetOfFiles", [])}
        self.owners = defaultdict(list)
        mnemonics = {mnemonic} if isinstance(mnemonic, str) else set(mnemonic)
        for action in data.get("actions", []):
            if action.get("mnemonic") not in mnemonics:
                raise ValueError("unexpected runtime aquery mnemonic")
            config = self.configs.get(str(action.get("configurationId", 0)))
            if config is None:
                raise ValueError("runtime action without configuration")
            target = self.targets[str(action["targetId"])]
            key = identity(target["label"], config["checksum"])
            if rows is not None:
                row = rows.get(key)
                if (row is None or (rule_classes is not None and row["rule_class"] not in rule_classes)
                        or classes[str(target["ruleClassId"])] != row["rule_class"]
                        or row["is_tool"] != config.get("isTool", False)):
                    raise ValueError("runtime action outside original configured closure: " + key)
            self.owners[key].append(action)

    def files(self, action, field):
        return [self.artifacts[str(ident)] for ident in action.get(field, [])]

    def input_ids(self, action):
        """Resolve original artifact IDs; detect absent depsets and cycles."""
        files, visited = set(), set()
        pending = [(str(ident), False) for ident in action.get("inputDepSetIds", [])]
        entered = set()
        while pending:
            ident, returning = pending.pop()
            if returning:
                entered.remove(ident)
                visited.add(ident)
                continue
            if ident in entered:
                raise ValueError("cycle in runtime action input depsets")
            if ident in visited:
                continue
            item = self.input_sets[ident]
            entered.add(ident)
            pending.append((ident, True))
            files.update(str(i) for i in item.get("directArtifactIds", []))
            pending.extend((str(dep), False) for dep in item.get("transitiveDepSetIds", []))
        if files - self.artifacts.keys():
            raise ValueError("unknown artifact in action input depsets")
        return files

    def inputs(self, action):
        """Resolve one unit at a time; never cache the broad input set per TU."""
        return {self.artifacts[ident] for ident in self.input_ids(action)}


_RUNTIME_PP_SHELL = """compiler="$1"; pp="$2"; headers="$3"; shift 3
"$compiler" "$@" || {
    status=$?
    rm -f -- "$pp" "$headers" || :
    exit "$status"
}
"""


def runtime_compile_source(ordinary, cpp, language):
    """A compile mnemonic and a rule's language alone do not prove it has a TU."""
    expected_mnemonic = "Assemble" if language == "ASM" else "CppCompile"
    if cpp.get("mnemonic") != expected_mnemonic:
        raise ValueError("runtime action does not match configured language: " + language)
    cmd = cpp.get("arguments", [])
    out = ordinary.files(cpp, "outputIds")
    if (len(out) != 1 or not out[0].endswith(".pic.o") or out[0] in ordinary.tree_artifacts
            or len(cmd) < 5 or cmd[-4:-1] != ["-o", out[0], "-c"]
            or cpp.get("paramFiles")):
        raise ValueError("runtime compile does not have one recorded object and fixed compile tail")
    source = cmd[-1]
    ext = Path(source).suffix
    extensions = {"C": {".c"}, "CXX": {".cc", ".cpp", ".cxx", ".c++", ".C"}, "ASM": {".S"}}
    if language not in extensions or ext not in extensions[language] or source in ordinary.tree_artifacts:
        raise ValueError("runtime compile input does not match configured language: " + source)
    if source not in ordinary.inputs(cpp):
        raise ValueError("runtime compile source missing from action inputs: " + source)
    return source


def runtime_unit_binding(ordinary, preprocess, cpp, pp, language):
    """Require the PP to replay the exact compiler, source, options and inputs."""
    source = runtime_compile_source(ordinary, cpp, language)
    cmd = cpp["arguments"]
    argv = pp.get("arguments", [])
    results = preprocess.files(pp, "outputIds")
    pp_files = [f for f in results if f.endswith(".i")]
    header_files = [f for f in results if f.endswith(".headers")]
    if (len(results) != 2 or len(pp_files) != 1 or len(header_files) != 1
            or any(file in preprocess.tree_artifacts for file in results) or pp.get("paramFiles")):
        raise ValueError("runtime preprocessing requires one .i and one .headers output")
    output, headers = pp_files[0], header_files[0]
    expected_args = [cmd[0], output, headers] + cmd[1:-4] + [
        "-o", output, "-E", source,
        "-dI", "-fshow-skipped-includes", "-Xclang", "-header-include-file",
        "-Xclang", headers, "-Xclang", "-sys-header-deps",
    ]
    if (len(argv) < 4 or argv[0] not in ("/bin/bash", "/usr/bin/bash", "/bin/sh", "/usr/bin/sh")
            or argv[1:4] != ["-c", _RUNTIME_PP_SHELL, ""]
            or argv[4:] != expected_args):
        raise ValueError("runtime preprocessing command differs from original compiler action: " + source)
    if pp.get("environmentVariables", []) != cpp.get("environmentVariables", []):
        raise ValueError("runtime preprocessing environment differs from original compiler action: " + source)
    if preprocess.inputs(pp) != ordinary.inputs(cpp):
        raise ValueError("runtime preprocessing inputs differ from CppCompile: " + source)
    return source


def validate_runtime_header_claims(row, claims, observed, original_declaration):
    """Bind owned files to the ordinary runtime rule, never to its input set alone."""
    expected = {"language", "units", "private_headers", "textual_srcs", "legacy_hdrs"}
    if (not isinstance(original_declaration, dict) or set(original_declaration) != expected
            or original_declaration["language"] != row["language"]):
        raise ValueError("invalid ordinary LLVM runtime input declaration")
    for field in expected - {"language"}:
        files = original_declaration[field]
        if (not isinstance(files, list) or len(set(files)) != len(files)
                or any(not isinstance(file, str) or not file or Path(file).is_absolute()
                       or ".." in Path(file).parts for file in files)):
            raise ValueError("invalid ordinary LLVM runtime declared " + field)
    units = original_declaration["units"]
    headers = original_declaration["private_headers"]
    textual = original_declaration["textual_srcs"]
    legacy = original_declaration["legacy_hdrs"]
    owned = units + headers + textual
    if (sorted(units) != sorted(observed) or len(set(owned)) != len(owned)
            or claims.get("runtime_private_headers") != headers
            or claims.get("runtime_textual_srcs") != textual
            or claims.get("public") != {}
            or claims.get("private") != {file: file for file in owned}
            or not isinstance(claims.get("required"), list)
            or sorted(claims["required"]) != sorted(owned)
            or claims.get("raw_assembly") != []
            or claims.get("unchecked_link_inputs") != []
            or claims.get("raw_link_candidates") != []
            or claims.get("unverified_compiler_inputs") != sorted(legacy)):
        raise ValueError("LLVM runtime owned manifest differs from its ordinary declared TUs/headers")
    return frozenset(headers + textual + legacy)


def runtime_declaration(declarations, key):
    """Read the unique ordinary rule declaration, with no aspect or tree artifact."""
    actions = declarations.owners.get(key, [])
    if len(actions) != 1:
        raise ValueError("runtime needs exactly one ordinary input declaration")
    action = actions[0]
    outputs = declarations.files(action, "outputIds")
    if (len(outputs) != 1 or not outputs[0].endswith(".llvm_runtime_inputs.json")
            or outputs[0] in declarations.tree_artifacts or action.get("aspectDescriptorIds")
            or declarations.inputs(action) or not isinstance(action.get("fileContents"), str)):
        raise ValueError("invalid ordinary runtime declaration FileWrite")
    return json.loads(action["fileContents"])


def audit_runtime_bindings(audit, roots, rows, eligible, headers, aspect, manifest):
    """Extra artifact/argv queries are confined to actual runtime compilations."""
    selected = {key: row for key, row in eligible.items() if runtime_compile_candidate(row)}
    if not selected:
        return {}
    scope = "kind('^llvm_runtime_objects( rule)?$', deps(" + query_labels(roots) + "))"
    include = ("--include_artifacts", "--include_commandline", "--include_param_files")
    ordinary_path = audit.query("runtime-ordinary-actions", "aquery",
                                "mnemonic('^(CppCompile|Assemble)$', " + scope + ")", include)
    preprocess_path = audit.query("runtime-preprocess-actions", "aquery",
                                  "mnemonic('^DwyuCompilerPreprocess$', " + scope + ")", (
                                      aspect, "--include_aspects", "--output_groups=dwyu",
                                  ) + include)
    declaration_path = audit.query(
        "runtime-declared-inputs", "aquery",
        "outputs('.*[.]llvm_runtime_inputs[.]json', " + scope + ")",
        ("--include_artifacts", "--include_file_write_contents"))
    ordinary = RuntimeActionGraph(ordinary_path, ("CppCompile", "Assemble"), rows)
    preprocess = RuntimeActionGraph(preprocess_path, "DwyuCompilerPreprocess", rows)
    declarations = RuntimeActionGraph(declaration_path, "FileWrite", rows)
    manifest["runtime_action_queries"] = {
        "ordinary": sha_file(ordinary_path),
        "preprocessing": sha_file(preprocess_path),
        "declared_inputs": sha_file(declaration_path),
    }
    # llvm_runtime_objects accepts legacy C/CXX sources without advertising
    # LlvmRuntimeCompileInfo when any source is not a C/CXX TU. Such a target
    # is still an unsupported real compile action, never an eligible CPP pass.
    for key, row in list(selected.items()):
        try:
            actions = ordinary.owners.get(key, [])
            if len(actions) != row["actions"][runtime_action_name(row)]:
                raise ValueError("runtime original compiler action count changed")
            sources = [runtime_compile_source(ordinary, action, row["language"]) for action in actions]
            if len(set(sources)) != len(sources):
                raise ValueError("runtime compiles one source more than once")
            row["category"] = "preprocessed_llvm_assembly" if runtime_asm_candidate(row) else "compiled_llvm_runtime"
        except (KeyError, TypeError, ValueError) as exc:
            row["category"] = "unsupported_compiled_or_cc_provider"
            row["runtime_source_binding"] = {"status": "not_proved", "reason": str(exc)}
            del selected[key]
            del eligible[key]
    manifest["runtime_preprocessors_not_eligible"] = sorted(set(preprocess.owners) - set(selected))
    result = {}
    for key, row in selected.items():
        result[key] = {"status": "not_proved"}
        try:
            expected_count = row["actions"][runtime_action_name(row)]
            compile_actions = ordinary.owners.get(key, [])
            preprocessed_actions = preprocess.owners.get(key, [])
            if len(compile_actions) != expected_count or len(preprocessed_actions) != expected_count:
                raise ValueError("runtime original compiler action and PP counts differ")
            compiled = {}
            for action in compile_actions:
                args = action.get("arguments", [])
                if len(args) < 5 or args[-1] in compiled:
                    raise ValueError("missing or duplicated runtime compile source")
                compiled[args[-1]] = action
            traced = {}
            for action in preprocessed_actions:
                args = action.get("arguments", [])
                # Eight fixed Clang trace options follow the actual source.
                if len(args) < 13 or args[-9] in traced:
                    raise ValueError("missing or duplicated runtime PP source")
                traced[args[-9]] = action
            if set(compiled) != set(traced):
                raise ValueError("runtime preprocessing omits or adds source files")
            observed = sorted(runtime_unit_binding(ordinary, preprocess, cpp, traced[src], row["language"])
                              for src, cpp in compiled.items())
            configured = headers.get(key, [])
            if len(configured) != 1:
                raise ValueError("runtime needs exactly one owned headers manifest")
            claims = configured[0]["contents"]
            declared = validate_runtime_header_claims(
                row, claims, observed, runtime_declaration(declarations, key))
            for action in compile_actions:
                if not set(declared).issubset(ordinary.inputs(action)):
                    # Every LLVM TU receives the family headers, textual sources,
                    # and raw hdrs. Neither provenance nor availability suffices alone.
                    raise ValueError("runtime declared input absent from original compiler action")
            units = []
            for source in observed:
                outputs = preprocess.files(traced[source], "outputIds")
                units.append({
                    "source": source,
                    "object": ordinary.files(compiled[source], "outputIds")[0],
                    "preprocessing": next(file for file in outputs if file.endswith(".i")),
                    "headers": next(file for file in outputs if file.endswith(".headers")),
                })
            if len({unit["object"] for unit in units}) != len(units):
                raise ValueError("runtime compiler actions share an output")
            result[key] = {"status": "matched_original_actions", "tu_count": len(observed),
                           "language": row["language"], "mnemonic": runtime_action_name(row),
                           "units": units, "action_queries": manifest["runtime_action_queries"]}
        except (KeyError, TypeError, ValueError) as exc:
            result[key]["reason"] = str(exc)
    return result


def audit_closure(audit, roots, rows, eligible, manifest):
    """Apply DWYU to roots in their original configurations, then audit each action."""
    expr = "outputs('.*(_dwyu_report[.]json|[.]dwyu_owned_headers[.]json)', deps(" + query_labels(roots) + "))"
    aspect = "--aspects=//bazel/rules:dwyu.bzl%" + (
        "dwyu_production_closure" if audit.production_census else "dwyu_closure")
    actions = audit.query("closure-analyzers", "aquery", expr, (
        aspect, "--include_aspects", "--include_artifacts", "--include_file_write_contents",
        "--include_commandline", "--include_param_files", "--output_groups=dwyu",
    ))
    owners, headers = configured_analyzers(actions)
    for key, row in eligible.items():
        if row["rule_class"] in NATIVE_CC:
            row["category"] = native_category(key, row, owners, headers)
    runtime_bindings = audit_runtime_bindings(audit, roots, rows, eligible, headers, aspect, manifest)
    manifest["runtime_analyzers_not_eligible"] = sorted(
        key for key in owners if key in rows
        and rows[key]["rule_class"] == "llvm_runtime_objects" and key not in eligible)
    manifest["analyzers_not_in_original_closure"] = sorted(set(owners) - set(rows))
    manifest["ambiguous_analyzers"] = {
        key: value for key, value in owners.items() if len(value) != 1
    }
    if audit.production_census:
        manifest["analyzers_outside_production"] = sorted(
            key for key in owners if key in rows and (
                rows[key]["is_tool"] or not rows[key]["owned_repository"]))
    info = audit.command("closure-execution-root", "info", ["execution_root"])
    if info["exit_code"]:
        raise RuntimeError("bazel info execution_root failed")
    execroot = Path(Path(info["stdout"]["file"]).read_text().strip())

    # Successful output qualification below requires the original BEP digest,
    # so the production audit need not remove cached output files.
    deleted = {}
    for key, paths in owners.items():
        for relative in paths:
            path = execroot / relative
            relative_output = path.relative_to(execroot / "bazel-out")
            if ".." in relative_output.parts:
                raise ValueError("DWYU output outside bazel-out: " + str(path))
            removed = path.is_file() or path.is_symlink()
            deleted[key] = removed and not audit.production_census
            if deleted[key]:
                path.unlink()
    bep = audit.out / "closure-strict.bep.json"
    native_bep = audit.out / "closure-ordinary.bep.json"
    proto_selected = {key: row for key, row in rows.items()
                      if row["rule_class"] == "cc_proto_library" and not row["is_tool"]
                      and (not audit.production_census or row["owned_repository"])}
    native = audit.command("closure-ordinary", "build", [
        "--keep_going", "--remote_download_outputs=minimal",
    ] + (["--build_event_publish_all_actions"] if proto_selected else []) + [
        "--build_event_json_file=" + str(native_bep),
    ] + roots)
    if audit.production_census:
        coverage = ordinary_reachability(native_bep, rows, roots)
        for key, row in eligible.items():
            covered = key in coverage["reachable"]
            row["ordinary"] = {
                "status": "passed" if covered else "not_proved",
                "reason": "reachable from an original successful configured root" if covered
                          else "no successful original configured root reaches this owner",
                "invocation_uuid": coverage["invocation_uuid"],
            }
        manifest["ordinary_coverage"] = {k: v for k, v in coverage.items() if k != "reachable"}
        manifest["ordinary_coverage"]["reachable_count"] = len(coverage["reachable"])
    else:
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
    completed_roots = defaultdict(list)
    expected_roots = {census_canonical(label) for label in roots}
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
            if canonical(target.get("label", "")) in expected_roots:
                key = identity(target["label"], target.get("configuration", {}).get("id", ""))
                if key not in rows or rows[key]["is_tool"]:
                    raise ValueError("DWYU closure root has unknown configured identity: " + key)
                completed_roots[key].append(event)
    remaining = []
    for key, completions in completed_roots.items():
        for event in select_completions(completions, True, command=run["command"], row=rows[key]):
            # Failed roots can carry fresh negative reports, but cannot attest
            # that a DWYU output was successfully produced.
            if event.get("completed", {}).get("success") is True and "aborted" not in event:
                remaining.extend(f["id"] for group in event["completed"].get("outputGroup", [])
                                 if group.get("name") == "dwyu" for f in group.get("fileSets", []))
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
            if name in named_files and named_files[name] != item:
                raise ValueError("inconsistent BEP named artifact: " + name)
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
        raw = path.read_bytes()
        report["report"] = {"file": str(path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
        candidates = [a["primaryOutput"] for _, a in actions_for_output if a.get("success") and "primaryOutput" in a]
        if relative in named_files:
            candidates.append(named_files[relative])
        matched_file = next((f for f in candidates if bep_file_matches(f, report["report"])), None)
        proved = matched_file is not None
        if proved and audit.production_census:
            report["bep_file"] = matched_file
        if audit.production_census and not proved:
            report["status"] = "not_proved"
            report["reason"] = "existing or failed output has no matching original successful BEP digest"
            continue
        source = json.loads(raw)
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
            validate_compiler_coverage(source)
            unverified_inputs = []
            if runtime_compile_candidate(row):
                unverified_inputs = validate_runtime_compiler_inputs(
                    source, configured_headers[0]["contents"])
        except ValueError as exc:
            report["status"] = "invalid"
            report["reason"] = str(exc)
            continue
        report["findings"] = {field: len(source[field]) for field in FIELDS}
        report["findings"]["unverified_link_deps"] = len(source.get("unverified_link_deps", []))
        if runtime_compile_candidate(row):
            report["compiler_binding"] = runtime_bindings[key]
        if runtime_compile_candidate(row) and runtime_bindings[key]["status"] != "matched_original_actions":
            report["status"] = "not_proved"
            report["reason"] = runtime_bindings[key].get("reason", "unmatched runtime compiler action")
        elif unverified_inputs:
            report["status"] = "unverified_compiler_inputs"
            report["reason"] = unverified_inputs
        elif source.get("unchecked_link_inputs") or configured_headers[0]["contents"]["unchecked_link_inputs"]:
            report["status"] = "unchecked_link_inputs"
        elif source["unentered_owned_files"]:
            report["status"] = "unentered_owned_files"
            report["reason"] = source["unentered_owned_files"]
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

    if audit.production_census:
        from audit_rust_import_dwyu import collect_consumer_evidence
        collect_consumer_evidence(audit, roots, rows, eligible, bep, execroot, aspect)

    from audit_textual_consumer_dwyu import collect as collect_textual_consumers
    collect_textual_consumers(audit, eligible, headers, execroot)

    # Keep generated protobuf C++ results separate from native and LLVM owners.
    # cc_proto_library itself remains a forwarding rule, never a CPP pass.
    if proto_selected:
        from audit_proto_dwyu import audit_proto
        proto = audit_proto(audit, roots, rows, proto_selected, native_bep, execroot)
        manifest["proto_cpp"] = proto
        for key, report in proto["compiled_owners"].items():
            rows[key]["category"] = "protobuf_generated_cpp"
            rows[key]["proto_cpp"] = report
        for key, report in proto["forwarders"].items():
            rows[key]["category"] = "protobuf_forwarder_no_cpp"
            rows[key]["proto_forwarder"] = report


def report_path(execroot, row):
    repo, suffix = row["label"].split("//", 1)
    pkg, name = suffix.split(":", 1)
    path = execroot / "bazel-out" / row["mnemonic"] / "bin"
    if repo:
        if not repo.startswith("@@"):
            raise ValueError("query returned apparent repository for " + row["label"])
        path = path / "external" / repo[2:]
    return path / pkg / (name + "_dwyu_report.json")


def dwyu_request(command):
    """Bind BEP aspect attributes to the complete DWYU command used by this audit."""
    requested = []
    parameters = dict(DWYU_PARAMETERS)
    if command is None:
        # Compatibility for callers which use the pinned --config=dwyu only.
        command = ["--config=dwyu"]
    for arg in command:
        if arg == "--config=dwyu":
            requested.append(DWYU_ASPECT + "dwyu")
        elif arg.startswith("--aspects="):
            requested.extend(arg[len("--aspects="):].split(","))
        elif arg.startswith("--aspects_parameters="):
            option = arg[len("--aspects_parameters="):]
            name, separator, value = option.partition("=")
            value = value.lower()
            if (not separator or name not in parameters or value not in ("true", "false")
                    or (name != "dwyu_verbose" and value != DWYU_PARAMETERS[name])):
                raise ValueError("unsupported or weakened DWYU aspect parameter: " + option)
            parameters[name] = value
        elif arg in ("--aspects", "--aspects_parameters", "--config"):
            raise ValueError("DWYU audit requires unambiguous --option=value command arguments")
    if (len(requested) != 1 or requested[0] not in
            {DWYU_ASPECT + name for name in DWYU_ASPECTS}):
        raise ValueError("DWYU command has no unique supported aspect")
    return requested[0], parameters


def select_completions(events, strict, *, command=None, row=None):
    """Return the requested aspect, or aspect-free ordinary completion only."""
    selected = []
    requested, expected = dwyu_request(command) if strict else (None, None)
    for event in events:
        target = event["id"]["targetCompleted"]
        if row is not None and identity(
                target.get("label", ""), target.get("configuration", {}).get("id", "")) != identity(
                    row["label"], row["checksum"]):
            raise ValueError("BEP completion has another owner or configuration")
        aspect = target.get("aspect", "")
        completed = event.get("completed", {})
        groups = [g.get("name") for g in completed.get("outputGroup", [])]
        if not strict:
            if not aspect and "dwyu" not in groups:
                selected.append(event)
            continue
        name, separator, attributes = aspect.partition("[")
        if name != requested:
            if "dwyu" in groups:
                raise ValueError("DWYU output group belongs to a different aspect")
            continue
        if not separator or not attributes.endswith("]"):
            raise ValueError("missing requested DWYU aspect parameters")
        fields = attributes[:-1].split(",")
        observed = {}
        for field in fields:
            key, equals, value = field.partition("=")
            if (not equals or key not in expected or key in observed
                    or value not in ('"true"', '"false"')):
                raise ValueError("invalid or duplicate requested DWYU aspect parameter")
            observed[key] = value[1:-1]
        if observed != expected:
            raise ValueError("BEP DWYU aspect parameters differ from the original command")
        if groups.count("dwyu") > 1:
            raise ValueError("duplicate DWYU output group")
        if len(selected) != 0:
            raise ValueError("duplicate configured DWYU completion")
        selected.append(event)
    if not strict and len(selected) > 1:
        raise ValueError("duplicate ordinary configured completion")
    return selected


def read_strict(row, events, path, original_deleted, filesets, *, command=None, keep_bep_file=False):
    selected = select_completions(events, True, command=command, row=row)
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
        validate_compiler_coverage(report)
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
            if len(matches) != 1 or not bep_file_matches(matches[0], result["report"],
                                                         local_path=path if keep_bep_file else None):
                raise ValueError("fresh report lacks matching successful BEP digest/length")
            if keep_bep_file:
                result["bep_file"] = matches[0]
        if report.get("unchecked_link_inputs"):
            result["status"] = "unchecked_link_inputs"
        elif report["unentered_owned_files"]:
            result["status"] = "unentered_owned_files"
            result["reason"] = report["unentered_owned_files"]
        elif report["unverified_compilation_deps"]:
            result["status"] = "unverified_compilation_deps"
            result["reason"] = report["unverified_compilation_deps"]
        elif any(result["findings"].values()):
            result["status"] = "findings"
        elif not all(e.get("completed", {}).get("success") for e in selected):
            result["status"] = "failed"
            result["reason"] = "zero findings but aspect failed"
        elif runtime_compile_candidate(row):
            # A top-level BEP alone does not bind the DWYU command to the
            # original compile action. The configured-closure audit does.
            result["status"] = "not_proved"
            result["reason"] = "runtime compiler audit needs the configured-closure action binding"
        else:
            result["status"] = "clean"
    except (ValueError, AttributeError, KeyError, TypeError) as exc:
        result["status"] = "invalid"
        result["reason"] = str(exc)
    return result


def emit_owner_records(out, records):
    """Bound the transmitted report independently of the number of Bazel invocations."""
    path = out / "declared-owners.jsonl.gz"
    count = 0
    with path.open("xb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as packed:
        for record in records:
            line = (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
            count += len(line)
            if count > 128 * 1024**2:
                raise ValueError("declared owner records exceed 128 MiB before compression")
            packed.write(line)
    stamp = sha_file(path)
    if stamp["bytes"] > 32 * 1024**2:
        raise ValueError("declared owner records exceed 32 MiB compressed")
    with path.open("rb") as data:
        for index, chunk in enumerate(iter(lambda: data.read(18_000), b"")):
            print("DWYU_OWNER_CHUNK=" + json.dumps({"archive_sha256": stamp["sha256"], "index": index,
                  "sha256": hashlib.sha256(chunk).hexdigest(),
                  "data": base64.b64encode(chunk).decode("ascii")}, separators=(",", ":")), flush=True)
    return {**stamp, "uncompressed_bytes": count, "chunks": (stamp["bytes"] + 17_999) // 18_000}


def primary_finding(report, row):
    if not any(report[field] for field in FIELDS):
        return None
    if (row["rule_class"] in ("cc_binary", "cc_test")
            and report["unused_deps"] == ["@@rules_cc+//:link_extra_lib"]
            and all(not report[field] for field in FIELDS if field != "unused_deps")):
        return "link_extra_lib_only"
    return "findings"


def audit_declared_owners(audit, census, selected, extra_fixtures):
    """Audit actual top-level C/C++ declarations once in this invocation's configuration."""
    indexed = subprocess.check_output(["git", "write-tree"], text=True).strip()
    revision_tree = subprocess.check_output(["git", "rev-parse", census["revision"] + "^{tree}"], text=True).strip()
    if indexed != census["tree"] or indexed != revision_tree:
        raise ValueError("Git index, revision and declared census tree disagree")
    subprocess.run(["git", "diff-files", "--quiet", "--ignore-submodules=dirty"], check=True)
    fixtures = set(census["fixture_declarations_not_scored"])
    declarations = set(census["production"])
    allocator = {label for label in declarations if label.startswith("//bazel/fixtures/allocator_selection:")}
    fixtures.update(allocator)
    production = declarations - allocator
    labels = set(selected) if selected else declarations
    if len(selected) != len(set(selected)) or len(extra_fixtures) != len(set(extra_fixtures)):
        raise ValueError("duplicate requested declaration")
    if not labels <= production | fixtures or not set(extra_fixtures) <= fixtures:
        raise ValueError("requested owner is outside the source census or fixture package")
    labels.update(extra_fixtures)
    apparent = {census_canonical(label): label for label in labels}
    if len(labels) != len(apparent):
        raise ValueError("census labels have duplicate canonical identities")
    expr = query_labels(sorted(labels))
    target_config = "config(" + expr + ", target)"
    rows, omitted = read_cquery(audit.query("requested", "cquery", target_config))
    if (omitted or len(rows) != len(labels) or {row["label"] for row in rows.values()} != set(apparent)
            or any(row["is_tool"] or row["rule_class"] not in NATIVE_CC for row in rows.values())):
        raise ValueError("requested C/C++ declarations differ from configured target identities")
    selected_headers = getattr(audit.args, "standalone_header_owner", ())
    header_labels = {census_canonical(label) for label in selected_headers}
    header_graphs = {}
    if selected_headers:
        import audit_public_headers as public_headers
        if (len(header_labels) != len(selected_headers) or len(header_labels) > public_headers.MAX_OWNERS
                or not set(selected_headers) <= labels):
            raise ValueError("standalone header owners must be distinct, requested labels (at most eight)")
    incompatible, providers = query_compatibility(audit, "requested-platform", target_config, rows, header_labels)
    active = {key: row for key, row in rows.items() if not incompatible[key]}
    analyzers, headers, ordinary, ordinary_events, strict, strict_events, filesets = {}, {}, {}, {}, {}, {}, {}
    ready = {}
    accounting, errors = {}, {}
    def record_error(name, error):
        errors[name] = (type(error).__name__ + ": " + str(error))[:1024]
        failure = {"name": name, "error": errors[name]}
        if name in ("requested-actions", "requested-cpp-outputs") and name not in accounting:
            accounting[name] = failure
            print("DWYU_QUERY_OWNER_CONFIGS=" + json.dumps(failure), flush=True)
        print("DWYU_DECLARED_EVIDENCE_ERROR=" + json.dumps(failure), flush=True)

    # These operations inspect evidence. Source identity, target configuration
    # and platform compatibility have already been checked above.
    evidence_errors = (ValueError, KeyError, TypeError, AttributeError, IndexError, RuntimeError, OSError)
    if active:
        roots = sorted(apparent[row["label"]] for row in active.values())
        expr = query_labels(roots)
        name = "requested-actions"
        try:
            graph = declared_action_graph(audit.query(name, "aquery", expr), active, name, accounting)
            if add_actions(graph, active):
                raise ValueError("ordinary action missing from configured target rows")
            del graph
        except evidence_errors as exc:
            record_error(name, exc)
        outputs = "outputs('.*(_dwyu_report[.]json|[.]dwyu_owned_headers[.]json)$', " + expr + ")"
        name = "requested-cpp-outputs"
        if header_labels:
            try:
                public_headers.check_sources(Path("."))
                audit.standalone_header_labels = public_headers.candidates(active, header_labels)
                audit.standalone_header_outputs = [str(report_path(Path(), row)) for row in active.values()
                                                   if row["label"] in audit.standalone_header_labels]
            except evidence_errors as exc:
                for key, row in active.items():
                    if row["label"] in header_labels:
                        header_graphs[key] = {"reason": str(exc)}
        try:
            query = audit.query(name, "aquery", outputs, (
                "--config=dwyu", "--include_aspects", "--include_artifacts", "--include_file_write_contents",
                "--include_commandline", "--include_param_files"))
            capture = getattr(audit, "standalone_header_labels", ())
            if capture and query.stat().st_size > public_headers.MAX_GRAPH_BYTES:
                raise ValueError("requested header action graph exceeds 32 MiB")
            graph = declared_action_graph(query, active, name, accounting)
            if capture:
                header_actions = public_headers.configured_header_graph(graph, active, capture)
                graph = dict(graph, actions=[a for a in graph["actions"]
                                             if a["mnemonic"] not in (public_headers.PP, public_headers.PROCESS)])
            analyzers, headers = configured_analyzers(graph)
            if capture:
                for key, row in active.items():
                    if row["label"] in capture:
                        try:
                            report, = analyzers[key]
                            manifest, = headers[key]
                            header_graphs[key] = public_headers.verify_actions(header_actions, row, manifest, report, audit.args.config)
                            header_graphs[key]["provider_query"] = providers["sha256"]
                            header_graphs[key]["action_query"] = accounting[name]["input"]["sha256"]
                        except evidence_errors as exc:
                            header_graphs[key] = {"reason": str(exc)[:512]}
                del header_actions
            del graph
        except evidence_errors as exc:
            record_error(name, exc)
        execroot = None
        try:
            info = audit.command("execution-root", "info", ["execution_root"])
            directory = Path(Path(info["stdout"]["file"]).read_text().strip())
            if info["exit_code"] or not directory.is_absolute():
                raise ValueError("execution_root unavailable")
            execroot = directory
        except evidence_errors as exc:
            record_error("execution-root", exc)
        try:
            ordinary, ordinary_events, _ = audit.build("ordinary", roots, False)
        except evidence_errors as exc:
            record_error("ordinary", exc)
        try:
            for key, row in active.items():
                row["category"] = native_category(key, row, analyzers, headers)
                if not execroot or not analyzers.get(key):
                    continue
                relative, = analyzers[key]
                path = execroot / relative
                if path != report_path(execroot, row) or ".." in path.parts:
                    raise ValueError("unexpected DWYU report: " + relative)
                existed = path.exists() or path.is_symlink()
                if existed:
                    path.unlink()
                ready[key] = (path, existed)
        except evidence_errors as exc:
            ready.clear()
            record_error("configured-reports", exc)
        # The selected target patterns do not depend on whether aquery found
        # their analyzers. A failed query cannot cancel the requested builds.
        try:
            strict, strict_events, filesets = audit.build("strict", roots, True)
        except evidence_errors as exc:
            record_error("strict", exc)
        if (not ordinary.get("invocation_uuid") or not strict.get("invocation_uuid") or
                ordinary["invocation_uuid"] == strict["invocation_uuid"]):
            record_error("build-identities", ValueError("missing or repeated ordinary/strict build identity"))

    statuses, categories, actionables, original_bytes = Counter(), Counter(), [], 0
    summaries = {"fixture": Counter(), "production": statuses}
    for key, row in sorted(rows.items()):
        label = apparent[row["label"]]
        row["scope"] = "fixture" if label in fixtures else "production"
        row["category"] = row.get("category", "incompatible" if incompatible[key] else "unverified")
        category = row["category"]
        ordinary_completed = select_completions(ordinary_events.get(key, []), False, row=row)
        row["ordinary"] = {"status": ("passed" if ordinary_completed[0].get("completed", {}).get("success") is True
                                      else "failed") if ordinary_completed else "not_tested",
                           "completion": ordinary_completed, "invocation_uuid": ordinary.get("invocation_uuid")}
        status = row["strict"] = {"status": "incompatible" if incompatible[key] else "not_tested"}
        if key in ready and strict.get("invocation_uuid"):
            path, existed = ready[key]
            if path.is_file():
                original_bytes += path.stat().st_size
                if path.stat().st_size > 16 * 1024**2 or original_bytes > 96 * 1024**2:
                    raise ValueError("DWYU report sizes exceed 16 MiB per owner or 96 MiB in total")
            status = row["strict"] = read_strict(row, strict_events.get(key, []), path, existed, filesets,
                                                  command=strict["command"], keep_bep_file=True)
            status["invocation_uuid"] = strict["invocation_uuid"]
            configured = headers[key][0]
            status["configured_headers"] = {"artifact": configured["artifact"]}
            status["failed_actions"] = strict["failed_actions"].get(key, [])
            if status.get("report"):
                original = path.read_bytes()
                if len(original) != status["report"]["bytes"] or hashlib.sha256(original).hexdigest() != status["report"]["sha256"]:
                    raise ValueError("strict report changed: " + key)
                checked = json.loads(original)
                status["original_json"] = original.decode("utf-8")
                finding = primary_finding(checked, row) if status["status"] != "invalid" else None
                if status["status"] == "findings":
                    status["status"] = finding or "unverified_link_deps"
                if status["status"] == "clean":
                    if configured["contents"].get("unverified_compiler_inputs"):
                        status["status"] = "unverified_compiler_inputs"
                    elif configured["contents"]["unchecked_link_inputs"]:
                        status["status"] = "unchecked_link_inputs"
                    elif configured["contents"]["raw_assembly"]:
                        status["status"] = "unchecked_assembly"
                if (status["status"] != "invalid" and status.get("completions") and not status.get("bep_file")
                        and not any(a["type"] == "DwyuAnalyzeTarget" and a["output"] == analyzers[key][0]
                                    and a["exit_code"] for a in status["failed_actions"])):
                    status["status"] = "unproved_failed_report"
                if status["status"] not in ("invalid", "unproved_failed_report") and finding == "findings":
                    actionables.append(label)
        outcome = status["status"]
        if category == "native_forwarder_no_sources" and outcome == "not_tested":
            outcome = "no_cpp"
        elif category == "header_only_native" and outcome == "clean":
            outcome = "header_pp_not_verified"
        elif category != "compiled_native" and outcome == "clean":
            outcome = "no_compiled_cpp_proof"
        if not incompatible[key] and row["ordinary"]["status"] != "passed":
            outcome = "ordinary_" + row["ordinary"]["status"]
        if category == "compiled_native" and row["actions"].get("CppModuleCompile") and outcome == "clean":
            outcome = "unchecked_modules"
        if row["label"] in header_labels:
            proof = header_graphs.get(key, {})
            if (outcome == "header_pp_not_verified" and not errors and (proof.get("headers") or proof.get("textual_files"))
                    and public_headers.successful_report(row, proof, ordinary, strict)):
                if proof.get("headers"):
                    outcome = "header_pp_verified"
                status["standalone_headers"] = proof
            else:
                status["standalone_headers"] = {"reason": proof.get("reason") or
                    row.get("standalone_headers", {}).get("error") or "no complete configured header actions and successful report"}
        if errors and outcome in ("clean", "no_cpp", "header_pp_not_verified", "no_compiled_cpp_proof"):
            outcome = "unverified_query_accounting"
        row["outcome"] = outcome
        summaries[row["scope"]][outcome] += 1
        if row["scope"] == "production":
            categories[category] += 1

    if audit.args.declared_contexts and ready and not errors:
        # The closure auditor also invokes these two readers. They attach their
        # own evidence and cannot turn an unentered header into a compiled C++ pass.
        from audit_rust_import_dwyu import CONSUMERS, collect_consumer_evidence
        from audit_textual_consumer_dwyu import collect as collect_textual_consumers
        eligible = {k: rows[k] for k in ready}
        roots = [r["label"] for r in eligible.values()]
        imports = [k for k, (owner, _) in CONSUMERS.items() if owner in roots]
        with_imports = dict(rows)
        if imports:
            imported, omitted = read_cquery(audit.query("rust-ffi-imports", "cquery",
                                                        "config(" + query_labels(imports) + ", target)"))
            if (omitted or len(imported) != len(imports) or {r["label"] for r in imported.values()} != set(imports)
                    or any(r["is_tool"] or r["rule_class"] != "cc_import" for r in imported.values())):
                raise ValueError("unexpected configured Rust imports")
            with_imports.update(imported)
            collect_consumer_evidence(audit, roots, with_imports, eligible, Path(strict["bep"]["file"]),
                                      execroot, "--config=dwyu")
        collect_textual_consumers(audit, eligible, headers, execroot)

    def records():
        for key, row in sorted(rows.items()):
            yield {"identity": key, "label": apparent[row["label"]], **{
                field: row[field] for field in ("checksum", "scope", "category", "rule_class", "sources",
                                                "actions", "ordinary", "strict", "outcome")}}
    capture = emit_owner_records(audit.out, records())
    production_rows = [r for r in rows.values() if r["scope"] == "production"]
    scored = [r for r in production_rows if r["category"] == "compiled_native"]
    return {"source": {k: census[k] for k in ("revision", "tree", "inventory_sha256", "source")},
            "config": audit.args.config, "scope": "selected declarations" if selected else "all declared C/C++ owners",
            "declared_production_total": len(production), "requested": len(labels), "categories": dict(categories),
            "omitted_production": len(production - labels), "omitted_fixtures": len(fixtures - labels),
            "ignored_other_overlay": census["ignored_declarations"],
            "ordinary_invocation": ordinary.get("invocation_uuid"), "strict_invocation": strict.get("invocation_uuid"),
            "query_accounting": accounting, "accounting_errors": errors,
            "statuses": {k: dict(v) for k, v in summaries.items()}, "records": capture, "runs": audit.runs,
            "actionable_production": sorted(set(actionables) - fixtures),
            "compiled_owners_clean": not errors and bool(scored) and all(r["outcome"] == "clean" for r in scored),
            "incomplete_build_evidence": [r["label"] for r in production_rows if
                r["label"] in header_labels and r["outcome"] != "header_pp_verified" or
                r["category"] != "incompatible" and (errors or r["ordinary"]["status"] != "passed" or
                r["outcome"] in ("invalid", "missing", "failed", "unproved_failed_report") or
                r["actions"].get("CppCompile") and r["category"] != "compiled_native")],
            **({"standalone_headers_requested": sorted(selected_headers),
                "standalone_headers_verified": sum(len(r["strict"]["standalone_headers"]["headers"]) for r in rows.values()
                                                  if r["outcome"] == "header_pp_verified")} if selected_headers else {}),
            "consumer_evidence_requested": audit.args.declared_contexts,
            "unverified_scopes": ["header-only preprocessing", "generated C++", "assembly", "tool configurations",
                                  "fixture expectations", "Rust import ownership", "textual consumer contexts"],
            "whole_repository_clean": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, choices=("cmake_match", "cmake_match_aarch64"))
    parser.add_argument("--out", required=True, type=Path, help="new output directory; raw queries stay here")
    parser.add_argument("--seed", action="append", help="explicit binary/standalone/test root, default //:clickhouse")
    parser.add_argument("--audit-target", action="append", default=[],
                        help="audit only these explicitly chosen reachable owners; others remain not_tested")
    parser.add_argument("--enumerate-only", action="store_true")
    parser.add_argument("--production-census", type=Path,
                        help="SHA-bound prepared inventory of owned ClickHouse BUILD declarations")
    parser.add_argument("--production-census-sha256",
                        help="SHA-256 of --production-census from the prepared source record")
    parser.add_argument("--declared-owners", action="store_true",
                        help="audit top-level declarations in one configuration; --audit-target selects a subset")
    parser.add_argument("--add-fixture", action="append", default=[], metavar="LABEL",
                        help="add a declared fixture to --declared-owners; report fixture results separately")
    parser.add_argument("--standalone-header-owner", action="append", default=[], metavar="LABEL",
                        help="verify the exported standalone C++ header preprocessing of a requested owner (repeatable)")
    parser.add_argument("--declared-contexts", action="store_true",
                        help="also inspect the configured Rust imports and textual header consumers (extra queries)")
    args = parser.parse_args()
    if bool(args.production_census) != bool(args.production_census_sha256):
        parser.error("--production-census and --production-census-sha256 must be supplied together")
    if args.declared_owners and (not args.production_census or args.seed or args.enumerate_only):
        parser.error("--declared-owners requires the source census and does not take --seed or --enumerate-only")
    if args.standalone_header_owner and not args.declared_owners:
        parser.error("--standalone-header-owner requires --declared-owners")
    if (args.add_fixture or args.declared_contexts) and not args.declared_owners:
        parser.error("--add-fixture and --declared-contexts require --declared-owners")
    if args.production_census and not args.declared_owners and (args.audit_target or args.seed):
        parser.error("production census discovers its own roots; --audit-target and --seed would reduce coverage")
    if Path(".bazelversion").read_text().strip() != "9.3.0":
        raise SystemExit("This inventory requires the pinned Bazel 9.3.0 analysis_v2 schema")
    audit = Audit(args)
    if args.declared_owners:
        census = read_production_census(args.production_census, args.production_census_sha256)
        summary = audit_declared_owners(audit, census, args.audit_target, args.add_fixture)
        path = audit.out / "manifest.json"
        write_json(path, summary)
        print("DWYU_DECLARED_OWNERS=" + json.dumps(summary, sort_keys=True, separators=(",", ":")), flush=True)
        return 0 if (summary["compiled_owners_clean"] and not summary["incomplete_build_evidence"]
                     and not summary["actionable_production"]) else 1
    roots = args.seed or ["//:clickhouse"]
    manifest = {"roots": roots, "scope": "configured closure of explicit roots only, not whole repository",
                "config": args.config, "bazel_version": "9.3.0", "runs": audit.runs,
                "rules": {}, "audit_passed": False}
    try:
        if args.production_census:
            census = read_production_census(args.production_census, args.production_census_sha256)
            # Use the same build setting for every production query and both
            # ordinary/strict builds so their configuration checksums match.
            audit.options.append("--//bazel/rules:dwyu_production_repositories=" +
                                 ",".join(census["owned_repositories"]))
            roots, discovered = choose_production_roots(audit, census, ["//:clickhouse", "//src:unit_tests_dbms"])
            manifest["scope"] = "owned ClickHouse C/C++ declarations and their complete configured dependency closure"
            manifest["roots"] = roots
            manifest["production_census"] = census
            manifest["discovered_roots"] = discovered
        expr = "deps(" + query_labels(roots) + ")"
        cq = audit.query("closure", "cquery", expr)
        rows, skipped = read_cquery(cq)
        manifest["rules"] = rows
        manifest["unconfigured_and_files"] = skipped
        if args.production_census:
            declared = {census_canonical(label) for label in census["production"]}
            reached = {row["label"] for row in rows.values() if not row["is_tool"]}
            incompatible = {r["label"] for r in discovered["incompatible"].values()}
            manifest["census_unreached_declarations"] = sorted(declared - reached - incompatible)
            if manifest["census_unreached_declarations"]:
                raise ValueError("final closure omitted census declarations: " +
                                 repr(manifest["census_unreached_declarations"]))
        aq = audit.query("actions", "aquery", expr)
        manifest["actions_not_matched_to_configured_rules"] = add_actions(aq, rows)
        eligible = {}
        for key, row in rows.items():
            if args.production_census:
                row["owned_repository"] = label_repository(row["label"]) in census["owned_repositories"]
            if row["is_tool"]:
                row["category"] = "tool_configuration"
            elif args.production_census and not row["owned_repository"]:
                row["category"] = "unowned_closure"
            elif row["rule_class"] in NATIVE_CC:
                row["category"] = native_category(key, row)
                if all(count is not None for count in row["sources"].values()) and any(row["sources"].values()):
                    eligible[key] = row
            elif runtime_compile_candidate(row):
                row["category"] = ("llvm_runtime_assembly_candidate" if runtime_asm_candidate(row)
                                   else "llvm_runtime_compiler_candidate")
                eligible[key] = row
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
                declared = audit.query("requested-cpp-outputs", "aquery",
                                       "outputs('.*(_dwyu_report[.]json|[.]dwyu_owned_headers[.]json)', "
                                       + query_labels(labels) + ")", (
                                           "--config=dwyu", "--include_aspects", "--include_artifacts",
                                           "--include_file_write_contents", "--include_commandline",
                                           "--include_param_files", "--remote_download_outputs=minimal"))
                owners, headers = configured_analyzers(declared)
                for key, row in selected.items():
                    if row["rule_class"] in NATIVE_CC:
                        row["category"] = native_category(key, row, owners, headers)
                native, native_events, _ = audit.build("ordinary", labels, False)
                for key, row in selected.items():
                    completed = select_completions(native_events.get(key, []), False,
                                                   command=native["command"], row=row)
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
                                                report_path(execroot, row), deleted[key], filesets,
                                                command=strict["command"])
                    row["strict"]["invocation_uuid"] = strict.get("invocation_uuid")
                    if not strict_events.get(key):
                        alternatives = [k for k in strict_events if k.startswith(row["label"] + " (")]
                        if alternatives:
                            row["strict"] = {"status": "configuration_not_reproduced",
                                             "actual_strict_identities": alternatives,
                                             "invocation_uuid": strict.get("invocation_uuid")}
                from audit_textual_consumer_dwyu import collect as collect_textual_consumers
                collect_textual_consumers(audit, selected, headers, execroot)
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
        blockers = ("unresolved_native_sources", "unsupported_compiled_or_cc_provider", "unowned_closure")
        manifest["audit_passed"] = bool(eligible) and not manifest["tool_cc_rules"] and not manifest.get(
            "ambiguous_analyzers") and not manifest.get("analyzers_not_in_original_closure") and not any(
            row["category"] in blockers for row in rows.values()) and not manifest[
                "actions_not_matched_to_configured_rules"] and not manifest.get(
                    "requested_not_in_eligible_closure") and not manifest.get(
                    "runtime_analyzers_not_eligible") and not manifest.get(
                    "runtime_preprocessors_not_eligible") and all(
                        row["ordinary"]["status"] == "passed" and row["strict"]["status"] == "clean"
                        for row in eligible.values())
        if args.production_census:
            # The universal result above remains false for unchecked tools and
            # unsupported providers. Do not rename these providers to 'clean'.
            # Unknown non-native C++ compile actions prevent a production pass.
            unsupported_cpp = {
                key: row for key, row in rows.items()
                if row["owned_repository"] and not row["is_tool"] and row["rule_class"] not in NATIVE_CC
                and key not in eligible
                and key not in manifest.get("proto_cpp", {}).get("compiled_owners", {})
                and any(name in ("CppCompile", "CppCompileHeader", "CppModuleCompile")
                        for name in row["actions"])
            }
            unsupported_llvm_assembly = {
                key for key, row in rows.items()
                if row["owned_repository"] and not row["is_tool"]
                and row["rule_class"] == "llvm_runtime_objects"
                and row["actions"].get("Assemble", 0) and key not in eligible
            }
            manifest["unsupported_llvm_assembly"] = sorted(unsupported_llvm_assembly)
            assembly = {key: row for key, row in eligible.items() if runtime_asm_candidate(row)}
            manifest["llvm_assembly_passed"] = not unsupported_llvm_assembly and all(
                row["ordinary"]["status"] == "passed" and row["strict"]["status"] == "clean"
                for row in assembly.values()
            )
            manifest["unsupported_production_cpp"] = sorted(unsupported_cpp)
            manifest["production_cpp_passed"] = (
                any(not runtime_asm_candidate(row) for row in eligible.values()) and not unsupported_cpp
                and manifest.get("proto_cpp", {"scope_complete": True})["scope_complete"]
            ) and not any(
                row["category"] == "unresolved_native_sources" for row in rows.values()
            ) and not any(manifest.get(field) for field in (
                "ambiguous_analyzers", "analyzers_not_in_original_closure",
                "analyzers_outside_production", "actions_not_matched_to_configured_rules", "census_unreached_declarations",
                "requested_not_in_eligible_closure", "runtime_analyzers_not_eligible",
                "runtime_preprocessors_not_eligible",
            )) and all(row["ordinary"]["status"] == "passed" and row["strict"]["status"] == "clean"
                      for row in eligible.values() if not runtime_asm_candidate(row))
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        manifest["error"] = type(exc).__name__ + ": " + str(exc)
    path = audit.out / "manifest.json"
    write_json(path, manifest)
    print("FULL_DWYU_MANIFEST=" + json.dumps({
        **sha_file(path), "audit_passed": manifest["audit_passed"],
        "production_cpp_passed": manifest.get("production_cpp_passed"),
        "categories": manifest.get("categories"), "ordinary": manifest.get("ordinary"),
        "strict": manifest.get("strict"), "error": manifest.get("error")
    }, sort_keys=True), flush=True)
    passed = (manifest.get("production_cpp_passed", False) and manifest.get("llvm_assembly_passed", False)) if args.production_census else manifest["audit_passed"]
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())

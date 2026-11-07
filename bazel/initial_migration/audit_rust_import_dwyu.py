"""Inspect Rust archives and their actual configured CPP FFI consumers."""

import gzip
import hashlib
import json
import os
from pathlib import Path
import re

from audit_full_dwyu import (
    FIELDS, NATIVE_CC, RuntimeActionGraph, _RUNTIME_PP_SHELL,
    bep_file_matches, canonical, identity, query_labels, read_cquery, select_completions, sha_file,
)
from audit_proto_dwyu import (
    ProtoBep, environment, expanded_args, graph_source, option, output_path, preprocess_options,
)


CONSUMERS = {
    "//bazel/rust:chdig_cc": (
        "//programs:clickhouse__compile_0", "programs/main.cpp"),
    "//bazel/rust:skim_cc": (
        "//src:dbms__compile_0", "src/Client/ReplxxLineReader.cpp"),
    "//bazel/rust:pco_cc": (
        "//src/Compression:clickhouse_compression", "src/Compression/CompressionCodecPco.cpp"),
    "//bazel/rust:prql_cc": (
        "//src/Parsers:clickhouse_parsers", "src/Parsers/PRQL/ParserPRQLQuery.cpp"),
    "//bazel/rust:polyglot_cc": (
        "//src/Parsers:clickhouse_parsers", "src/Parsers/Polyglot/ParserPolyglotQuery.cpp"),
    "//bazel/rust:delta_kernel_ffi_cc": (
        "//src:dbms__compile_0", "src/Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.cpp"),
}
SOURCE_PATTERN = "^(programs/main|src/(Client/ReplxxLineReader|Compression/CompressionCodecPco|Parsers/PRQL/ParserPRQLQuery|Parsers/Polyglot/ParserPolyglotQuery|Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils))[.]cpp$"
CPP_ACTIONS = (
    "DwyuRustImportEvidence", "DwyuProcessConsumerContext",
    "DwyuAnalyzeImportHeaders", "DwyuOwnedSymbols", "DwyuArchiveSymbols",
)
COMPACT_BYTES = 32 * 1024 * 1024
COMPACT_DECODED_BYTES = 128 * 1024 * 1024
COMPILE_CC_KEYS = {
    "public", "private", "textual", "available", "includes", "quote_includes",
    "system_includes", "external_includes", "owned_linker_inputs",
}


def header_files(cc):
    if not isinstance(cc, dict) or set(cc) != COMPILE_CC_KEYS:
        raise ValueError("cc_import has no complete original CcInfo")
    files = {}
    for kind in ("public", "private", "textual", "available"):
        value = cc[kind]
        if not isinstance(value, list):
            raise ValueError("cc_import CcInfo has non-list " + kind)
        for item in value:
            if (not isinstance(item, dict) or set(item) != {"file", "tree"} or
                    not isinstance(item["file"], str) or not item["file"] or
                    Path(item["file"]).is_absolute() or ".." in Path(item["file"]).parts or
                    type(item["tree"]) is not bool or item["tree"]):
                raise ValueError("unsupported cc_import header or tree artifact")
        files[kind] = {item["file"] for item in value}
    for kind in ("public", "private", "textual"):
        if not files[kind] <= files["available"]:
            raise ValueError("cc_import direct headers absent from original CcInfo")
    for kind in ("includes", "quote_includes", "system_includes", "external_includes"):
        if not isinstance(cc[kind], list) or any(not isinstance(value, str) for value in cc[kind]):
            raise ValueError("invalid cc_import include roots")
    return files


def owned_archive(row, cc):
    owned = cc["owned_linker_inputs"]
    if not isinstance(owned, list) or len(owned) != 1:
        raise ValueError("import must have exactly one owned linker input")
    linker = owned[0]
    if (not isinstance(linker, dict) or
            set(linker) != {"owner", "libraries", "flags", "additional_inputs"} or
            canonical(linker["owner"]) != row["label"] or linker["flags"] != [] or
            linker["additional_inputs"] != [] or not isinstance(linker["libraries"], list) or
            len(linker["libraries"]) != 1):
        raise ValueError("unsupported cc_import owned linker input")
    lib = linker["libraries"][0]
    if (not isinstance(lib, dict) or set(lib) != {
            "static_library", "pic_static_library", "dynamic_library", "interface_library",
            "objects", "pic_objects", "alwayslink"} or lib["alwayslink"] is not False or
            any(lib[k] for k in ("objects", "pic_objects", "dynamic_library", "interface_library"))):
        raise ValueError("unsupported cc_import archive kind; not a single non-alwayslink static library")
    archives = lib["static_library"] + lib["pic_static_library"]
    if (len(archives) != 1 or not isinstance(archives[0], dict) or
            set(archives[0]) != {"file", "tree"} or archives[0]["tree"] is not False):
        raise ValueError("cc_import must own one non-tree archive")
    path = output_path(archives[0]["file"])
    if not path.endswith(".a"):
        raise ValueError("cc_import did not own an archive: " + path)
    return path


def source_producer(key, rows, graph, archive, ordinary):
    """Only an original direct configured rust_archive may produce this archive."""
    deps = {
        identity(dep["label"], dep["configurationChecksum"])
        for dep in rows[key]["configured_inputs"] if dep.get("configurationChecksum")
    }
    candidates = []
    for parent in sorted(deps):
        for action in graph.owners.get(parent, ()):
            if archive in graph.files(action, "outputIds"):
                candidates.append((parent, action))
    if len(candidates) != 1:
        raise ValueError("import archive has no unique direct configured ClickHouseRustSymbols producer")
    owner, action = candidates[0]
    if (rows[owner]["rule_class"] != "rust_archive" or rows[owner]["is_tool"] or
            graph.files(action, "outputIds") != [archive] or archive in graph.tree_artifacts or
            action.get("aspectDescriptorIds")):
        raise ValueError("import archive did not come from an original rust_archive rule")
    args = action.get("arguments", [])
    if not isinstance(args, list) or not args or action.get("paramFiles"):
        raise ValueError("original ClickHouseRustSymbols command is missing or unexpanded")
    sources = [args[i + 1] for i, arg in enumerate(args[:-1]) if arg == "--source"]
    outputs = [args[i + 1] for i, arg in enumerate(args[:-1]) if arg == "--output"]
    if (len(sources) != 1 or not sources[0].endswith(".a") or
            sources[0] not in graph.inputs(action) or sources[0] in graph.tree_artifacts or
            outputs != [archive]):
        raise ValueError("ClickHouseRustSymbols has no unique original source archive and matching output")
    evidence = ordinary.output_evidence(owner, archive, "ClickHouseRustSymbols")
    if evidence is None:
        raise ValueError("original ordinary BEP has no successful import archive evidence")
    return {"producer": owner, "source_archive": sources[0], "import_archive": archive,
            "original_action_key": action.get("actionKey"), "ordinary_evidence": evidence}


def configured_consumer(audit, observe, options):
    expected = sorted({consumer for consumer, _ in CONSUMERS.values()})
    expr = "config(" + query_labels(expected) + ", target)"
    query = audit.query("rust-import-consumers", "cquery", expr)
    configurations = sha_file(query)
    consumers, omitted = read_cquery(query)
    if ({row["label"] for row in consumers.values()} != set(expected) or omitted or
            any(row["is_tool"] or row["rule_class"] not in NATIVE_CC for row in consumers.values())):
        raise ValueError("original Rust C++ consumers changed kind, target configuration, or ownership")
    info, info_query = observe(audit, expr, consumers, name="rust-import-consumer-platform")
    if any(item["incompatible"] for item in info.values()):
        raise ValueError("Rust header consumer incompatible with the requested platform")
    query = audit.query("rust-import-consumer-compile", "aquery",
                        "inputs('" + SOURCE_PATTERN + "', mnemonic('^CppCompile$', " +
                        query_labels(expected) + "))", options)
    graph = RuntimeActionGraph(query, "CppCompile", consumers, NATIVE_CC)
    return consumers, info, graph, {
        "consumer_actions": sha_file(query), "consumer_cquery": info_query,
        "consumer_configurations": configurations,
    }


def consumer_binding(key, row, files, consumers, info, graph):
    label = row["label"]
    target, source = CONSUMERS[label]
    candidates = [key for key, item in consumers.items() if item["label"] == target]
    if len(candidates) != 1:
        raise ValueError("original import consumer configuration is ambiguous")
    owner = candidates[0]
    dep_keys = {
        identity(dep["label"], dep["configurationChecksum"])
        for dep in consumers[owner]["configured_inputs"] if dep.get("configurationChecksum")
    }
    if key not in dep_keys:
        raise ValueError("C++ consumer does not directly use this configured import")
    cc = info[owner]["cc_info"]
    required = files["public"] | files["private"] | files["textual"]
    if not isinstance(cc, dict) or set(cc) != COMPILE_CC_KEYS or not isinstance(cc["available"], list):
        raise ValueError("original CPP consumer does not provide complete CcInfo")
    # A consumer's public CcInfo need not export headers from implementation_deps.
    # Record what it exports and require the import headers in the real compiler
    # inputs below, where private compilation dependencies are available.
    exported = {item["file"] for item in cc["available"]
                if isinstance(item, dict) and set(item) == {"file", "tree"}
                and item["tree"] is False}
    compilers = []
    for action in graph.owners.get(owner, []):
        original, obj = graph_source(graph, action)
        if original == source:
            compilers.append((action, obj))
    if len(compilers) != 1:
        raise ValueError("original consumer TU absent or has multiple compiler actions: " + source)
    action, obj = compilers[0]
    if not required <= graph.inputs(action):
        raise ValueError("import headers absent from original consumer compiler input depset")
    return {"consumer": owner, "source": source, "object": obj,
            "action_key": action.get("actionKey"), "direct_headers": sorted(required),
            "import_headers_in_consumer_public_cc_info": sorted(required & exported),
            "status": "original_consumer_compile_inputs_bound",
            "headers_qualified": False,
            "reason": "Compiler inputs do not prove the header was entered or its includes were direct."}


def check_imports(audit, keys, rows, observations, ordinary, observe, options, cpp_manifests=()):
    results = {key: {
        "label": rows[key]["label"], "checksum": rows[key]["checksum"],
        "cpp_qualified": False, "headers_qualified": False, "archive_producer_checked": False,
        "symbols_checked": False, "imports_checked": False, "status": "not_proved",
    } for key in keys}
    original = [rows[key]["label"] for key in keys]
    try:
        query = audit.query("rust-import-producers", "aquery",
                            "mnemonic('^ClickHouseRustSymbols$', deps(" + query_labels(original) + "))", options)
        graph = RuntimeActionGraph(query, "ClickHouseRustSymbols", rows, ("rust_archive",))
    except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
        for record in results.values():
            record["reason"] = "cannot inspect original Rust archive: " + str(exc)
        return results
    query_evidence = {"producer_actions": sha_file(query)}
    try:
        consumers, info, cpp_graph, consumer_queries = configured_consumer(audit, observe, options)
        query_evidence.update(consumer_queries)
        consumer_error = None
    except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
        consumers, info, cpp_graph = {}, {}, None
        consumer_error = str(exc)
    for key, report in results.items():
        row = rows[key]
        cc = observations[key]["cc_info"]
        report["queries"] = query_evidence
        try:
            if row["actions"]:
                raise ValueError("cc_import unexpectedly owns original compiler actions")
            files = header_files(cc)
            if key not in ordinary.successful_roots:
                raise ValueError("cc_import lacked a successful ordinary configured root")
            archive = owned_archive(row, cc)
            report["archive"] = source_producer(key, rows, graph, archive, ordinary)
            report["archive_producer_checked"] = True
            report["cc_info"] = cc
            required = files["public"] | files["private"] | files["textual"]
            report["headers_applicable"] = bool(required)
            if row["label"] not in CONSUMERS:
                raise ValueError("Rust import has no identified actual CPP consumer")
            if consumer_error:
                raise ValueError("cannot inspect original consumer: " + consumer_error)
            binding = consumer_binding(key, row, files, consumers, info, cpp_graph)
            report["consumer"] = binding
            ev = completed_consumer(cpp_manifests, binding["consumer"], row["label"])
            value = ev["observation"]
            if (ev["import"] != key or value["archive"] != archive or value["source"] != binding["source"] or
                    ev["object"] != binding["object"] or value["headers_applicable"] != bool(required)
                    or not required <= set(value["import_headers"])):
                raise ValueError("CPP import evidence differs from original configured archive, TU or headers")
            report["cpp_evidence"] = ev
            report["symbols_checked"] = bool(not value["limited"] and binding["object"] in value["object_matches"])
            no_header_findings = not (any(value["findings"][k] for k in FIELDS) or
                value["unentered_owned_files"] or value["unverified_compilation_deps"] or
                value["unverified_link_deps"])
            report["headers_qualified"] = bool(required) and no_header_findings
            report["imports_checked"] = report["symbols_checked"] and (not required or report["headers_qualified"])
            report["status"] = "import_observed" if report["imports_checked"] else "findings"
            report["reason"] = ("Actual CPP reference/selected archive definition only; no Rust CPP or final-link extraction credit."
                                if report["imports_checked"] else "Unentered or undeclared import headers, or no bound strong symbol match.")
        except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
            report["reason"] = str(exc)
    return results


def saved_compact(data, spec, body):
    """Keep the exact original emitted group bytes until the generated audit."""
    if len(body) > COMPACT_BYTES or data["phase"] != "candidate":
        raise ValueError("invalid current CPP byte stream")
    directory = Path(os.environ["CLICKHOUSE_NATIVE_FIRST_STATUS_DIR"]) / ("compact-" + data["arch"])
    directory.mkdir(exist_ok=True)
    name = hashlib.sha256(Path(spec).read_bytes()).hexdigest() + ".json.gz"
    with (directory / name).open("xb") as stream:
        stream.write(body)


def compact_manifest(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > COMPACT_BYTES:
        raise ValueError("missing, aliased or excessive compact CPP manifest")
    with gzip.open(path, "rb") as stream:
        raw = stream.read(COMPACT_DECODED_BYTES + 1)
        if len(raw) > COMPACT_DECODED_BYTES or stream.read(1):
            raise ValueError("compact CPP manifest exceeds uncompressed limit")
    manifest = json.loads(raw)
    if (set(manifest) != {"schema", "source", "snapshot", "tree", "phase", "arch", "groups",
                          "baseline_evidence", "whole_repository_clean"}
            or manifest["schema"] != 1 or manifest["phase"] != "candidate"
            or manifest["arch"] not in ("x86", "arm")
            or manifest["whole_repository_clean"] is not False
            or not isinstance(manifest["groups"], dict)
            or not set(manifest["groups"]) <= {"match", "rs"}):
        raise ValueError("not a completed original compact CPP audit")
    return manifest


def completed_consumer(manifests, owner, imported):
    """Match the original CPP and import reports to their successful named BEP files."""
    found = []
    for path in manifests:
        path = Path(path)
        if path.name.endswith(".json.gz"):
            manifest = compact_manifest(path)
            for group_name, group in manifest["groups"].items():
                if group.get("status") != "collected":
                    continue
                record = group.get("rows", {}).get(owner)
                if not record or record.get("ordinary", {}).get("status") != "not_tested":
                    continue
                strict = record.get("strict", {})
                evidence = strict.get("rust_imports", {}).get(imported)
                if not evidence or evidence.get("status") != "bound_original_cpp":
                    continue
                original = strict.get("report")
                cpp_bep = evidence.get("cpp_bep", {})
                if (original != cpp_bep.get("report") or
                        not bep_file_matches(cpp_bep.get("bep_file", {}), original) or
                        sha_file(Path(original["file"])) != original):
                    raise ValueError("original compact CPP report changed or has no successful BEP")
                checked = checked_consumer_report(evidence)
                found.append(dict(checked, cpp_manifest=sha_file(path), cpp_group=group_name,
                                  cpp_strict=original, consumer_ordinary=record["ordinary"]))
            continue
        manifest = json.loads(path.read_bytes())
        if not manifest.get("production_census") or manifest.get("error"):
            continue
        record = manifest.get("rules", {}).get(owner)
        if not record or record.get("ordinary", {}).get("status") != "passed":
            continue
        strict = record.get("strict", {})
        original = strict.get("report")
        if (strict.get("status") != "clean" or not original or
                not bep_file_matches(strict.get("bep_file", {}), original)):
            continue
        if sha_file(Path(original["file"])) != original:
            raise ValueError("original CPP report differs from successful BEP")
        evidence = strict.get("rust_imports", {}).get(imported)
        if not evidence or evidence.get("status") != "bound_original_cpp":
            continue
        checked = checked_consumer_report(evidence)
        found.append(dict(checked, cpp_manifest=sha_file(path), cpp_strict=original,
                          consumer_ordinary=record["ordinary"]))
    if len(found) != 1:
        raise ValueError("missing or ambiguous original CPP audit for " + imported + ": " + owner)
    return found[0]


def checked_consumer_report(evidence):
    info = evidence.get("bep", {})
    if not bep_file_matches(info.get("bep_file", {}), info.get("report", {})):
        raise ValueError("Rust CPP observation has no original successful BEP digest")
    actual = sha_file(Path(info["report"]["file"]))
    if actual != info["report"]:
        raise ValueError("Rust CPP observation differs from successful BEP")
    value = json.loads(Path(actual["file"]).read_bytes())
    if value != evidence.get("observation") or value.get("cpp_qualified") is not False:
        raise ValueError("Rust observation changed or assigned CPP credit to Rust")
    return evidence


def only_output(graph, actions, output, owner, mnemonic):
    found = [(key, action) for key, group in actions.items() for action in group
             if output in graph.files(action, "outputIds")]
    if (len(found) != 1 or found[0][0] != owner or
            found[0][1].get("mnemonic") != mnemonic or output in graph.tree_artifacts):
        raise ValueError("missing or wrong configured " + mnemonic + " output: " + output)
    action = found[0][1]
    descriptors = [graph.aspect_descriptors.get(str(i), {}) for i in action.get("aspectDescriptorIds", [])]
    permitted = ("//bazel/rules:dwyu.bzl%dwyu", "//bazel/rules:dwyu.bzl%dwyu_production_closure")
    if not any(d.get("name", "").endswith(permitted) for d in descriptors):
        raise ValueError("import observation did not come from original production CPP aspect")
    return action


def dwyu_arguments(action):
    # DWYU passes main-repository labels literally. Only its declared response
    # files, spelled @file or --param_file=file, may supply more arguments.
    arguments = [("@" + arg[len("--param_file="):] if arg.startswith("--param_file=")
                  else canonical(arg)) for arg in action.get("arguments", [])]
    return expanded_args(dict(action, arguments=arguments))


def native_cpp_preprocessing(cpp, original, pp, graph, source, obj, output, trace, owned_manifest):
    """Compare original native CPP to CPP DWYU, allowing preprocessing outputs only."""
    argv = expanded_args(pp)
    suffix = ["-dI", "-fshow-skipped-includes", "-Xclang", "-header-include-file",
              "-Xclang", trace, "-Xclang", "-sys-header-deps"]
    if (len(argv) < 17 or argv[0] not in ("/bin/bash", "/usr/bin/bash", "/bin/sh", "/usr/bin/sh")
            or argv[1:4] != ["-c", _RUNTIME_PP_SHELL, ""]
            or argv[4:9] != [cpp["arguments"][0], output, trace, "-x", "c++"]
            or argv[-len(suffix):] != suffix):
        raise ValueError("CPP FFI preprocessing changed compiler, language or trace options")
    body = argv[9:-len(suffix)]
    if body.count("-E") != 1 or "-c" in body or "-x" in body:
        raise ValueError("CPP FFI preprocessing has unknown language or compile options")
    reconstructed = [argv[4]] + ["-c" if arg == "-E" else arg for arg in body]
    wanted = preprocess_options(cpp["arguments"], original.files(cpp, "outputIds"), source, obj)
    actual = preprocess_options(reconstructed, graph.files(pp, "outputIds"), source, output)
    if wanted != actual or environment(cpp) != environment(pp):
        raise ValueError("CPP FFI preprocessing differs from original CPP flags or environment")
    original_inputs = original.inputs(cpp)
    pp_inputs = graph.inputs(pp)
    if not original_inputs <= pp_inputs or owned_manifest not in pp_inputs:
        raise ValueError("CPP FFI preprocessing drops original compiler inputs or ownership")


def check_archive_scan(graph, owners, archive, scan, ev_inputs):
    if not isinstance(scan, dict) or set(scan) != {"symbols", "status", "magic"}:
        raise ValueError("CPP FFI lacks the archive symbol, status and signature outputs")
    argv = [archive] + [output_path(scan[field]) for field in ("symbols", "status", "magic")]
    outputs = set(argv[1:])
    if len(outputs) != 3 or not set(argv) <= ev_inputs:
        raise ValueError("CPP FFI lacks the original archive or all three distinct scan outputs")
    # A shared scan belongs to the selected cc_import. An uncovered archive's
    # scan belongs to the original caller. Match the archive and each output to argv.
    scans = [a for key in owners for a in graph.owners.get(key, ())
             if a.get("mnemonic") == "DwyuArchiveSymbols" and archive in graph.inputs(a)
             and set(graph.files(a, "outputIds")) == outputs and expanded_args(a)[-4:] == argv]
    if len(scans) != 1:
        raise ValueError("CPP FFI lacks one original llvm-nm action for the configured archive")


def bind_cpp_observation(owner, imported, row, cpp, original, graph, preprocess, ev, value, rows):
    source, obj = graph_source(original, cpp)
    cmd = dwyu_arguments(ev)
    name = option(cmd, "--output")
    if (set(graph.files(ev, "outputIds")) != {name} or source != CONSUMERS[imported][1] or
            canonical(option(cmd, "--consumer")) != row["label"] or canonical(option(cmd, "--import")) != imported
            or option(cmd, "--source") != source or value.get("source") != source
            or canonical(value.get("consumer", "")) != row["label"] or canonical(value.get("import", "")) != imported
            or value.get("kind") != "RUST_IMPORT_CPP_OBSERVATION" or value.get("schema") != 1
            or value.get("cpp_qualified") is not False or value.get("final_link_extraction") is not False
            or type(value.get("limited")) is not bool or not isinstance(value.get("object_matches"), dict)
            or not isinstance(value.get("findings"), dict) or
            any(not isinstance(value["findings"].get(field), kind) for field, kind in FIELDS.items())):
        raise ValueError("CPP FFI evidence owner, original TU, role or report differs")
    output, trace = option(cmd, "--preprocessed"), option(cmd, "--compiler-headers")
    if value.get("preprocessed") != output or value.get("compiler_headers") != trace:
        raise ValueError("CPP FFI evidence reports different compiler artifacts")
    pp = only_output(preprocess, preprocess.owners, output, owner, "DwyuCompilerPreprocess")
    if only_output(preprocess, preprocess.owners, trace, owner, "DwyuCompilerPreprocess") is not pp:
        raise ValueError("CPP FFI preprocessor and skipped-header trace differ")
    header_report = option(cmd, "--header-report")
    header_action = only_output(graph, graph.owners, header_report, owner, "DwyuAnalyzeImportHeaders")
    hdr_cmd = dwyu_arguments(header_action)
    import_headers = option(cmd, "--import-headers")
    if (canonical(option(hdr_cmd, "--target_under_inspection")) != imported or
            option(hdr_cmd, "--owned_headers") != import_headers or option(hdr_cmd, "--output") != header_report):
        raise ValueError("import header analysis uses another header owner")
    pub, private = option(hdr_cmd, "--preprocessed_public_files"), option(hdr_cmd, "--preprocessed_private_files")
    processor = only_output(graph, graph.owners, pub, owner, "DwyuProcessConsumerContext")
    if (only_output(graph, graph.owners, private, owner, "DwyuProcessConsumerContext") is not processor or
            set(graph.files(processor, "outputIds")) != {pub, private}):
        raise ValueError("import header analysis has mismatched CPP include records")
    process = dwyu_arguments(processor)
    if (option(process, "--files") != output or option(process, "--compiler_headers") != trace or
            option(process, "--source") != source or option(process, "--owned_headers") != import_headers or
            option(process, "--mode") != "compiler" or process.count("--consumer_root") != 1):
        raise ValueError("import header reader did not use this real consumer and owner")
    strict = row["strict"]
    native_cpp_preprocessing(cpp, original, pp, preprocess, source, obj, output, trace,
                             strict["configured_headers"]["artifact"])
    ev_inputs = graph.inputs(ev)
    if not {output, trace, header_report, import_headers, option(cmd, "--symbols")} <= ev_inputs:
        raise ValueError("CPP FFI evidence inputs do not contain its own reports")
    archive = output_path(value["archive"])
    nm_objects = [a for a in graph.owners.get(owner, ())
                  if a.get("mnemonic") == "DwyuOwnedSymbols" and obj in graph.inputs(a)
                  and set(graph.files(a, "outputIds")) <= ev_inputs]
    deps = {identity(dep["label"], dep["configurationChecksum"])
            for dep in row["configured_inputs"] if dep.get("configurationChecksum")}
    imports = [key for key in deps if key in rows and rows[key]["label"] == imported and
               rows[key]["rule_class"] == "cc_import" and not rows[key]["is_tool"]]
    if len(imports) != 1:
        raise ValueError("CPP FFI lacks one original configured direct cc_import")
    if len(nm_objects) != 1:
        raise ValueError("CPP FFI lacks the original configured object llvm-nm action")
    check_archive_scan(graph, (imports[0], owner), archive, value.get("archive_scan"), ev_inputs)
    match = value["object_matches"].get(obj)
    if match is not None and (set(match) != {"symbol", "member"} or
            any(not isinstance(match[k], str) or not match[k] for k in ("symbol", "member"))):
        raise ValueError("CPP FFI returned invalid symbol observation")
    return {"status": "bound_original_cpp", "object": obj, "source": source,
            "original_cpp_action_key": cpp.get("actionKey"), "observation": value, "import": imports[0]}


def collect_consumer_evidence(audit, roots, rows, eligible, strict_bep, execroot, aspect,
                              compact=None):
    """Read only actual requested CPP callers in their already scheduled CPP batch."""
    wanted = {canonical(label) for label in roots} & {label for label, _ in CONSUMERS.values()}
    selected = {key: row for key, row in eligible.items() if row["label"] in wanted and
                (row.get("ordinary", {}).get("status") == "passed" and
                 row.get("strict", {}).get("status") == "clean" or
                 compact is not None and row.get("ordinary", {}).get("status") == "not_tested" and
                 row.get("strict", {}).get("report"))}
    if not selected:
        return
    bep = ProtoBep(strict_bep, roots, rows, "dwyu")
    successful = {}
    for key, row in selected.items():
        strict = row["strict"]
        strict["rust_imports"] = {
            imported: {"status": "not_proved", "strict_bep": bep.path, "invocation_uuid": bep.invocation_uuid}
            for imported, (consumer, _) in CONSUMERS.items() if consumer == row["label"]
        }
        reason = None
        if strict.get("invocation_uuid") != bep.invocation_uuid:
            reason = "FFI caller report is from a different strict invocation"
        elif key not in bep.successful_roots:
            reason = "FFI caller was not a successful original CPP root"
        elif compact is not None:
            try:
                completions = select_completions(strict["completions"], True, command=[aspect], row=row)
                if len(completions) != 1 or completions[0].get("completed", {}).get("success") is not True:
                    raise ValueError("FFI caller lacks its successful configured DWYU aspect completion")
                strict_path = Path(strict["report"]["file"]).relative_to(execroot).as_posix()
                _, cpp_bep = bep.read_report(execroot, key, strict_path)
                if cpp_bep["report"] != strict["report"]:
                    raise ValueError("FFI caller report differs from original strict result")
                for record in strict["rust_imports"].values():
                    record["cpp_bep"] = cpp_bep
            except (KeyError, ValueError, TypeError, OSError) as exc:
                reason = str(exc)
        if reason:
            for record in strict["rust_imports"].values():
                record["reason"] = reason
        else:
            successful[key] = row
    selected = successful
    if not selected:
        return
    wanted = {row["label"] for row in selected.values()}
    rows = dict(rows)
    imported = [key for key, (owner, _) in CONSUMERS.items() if owner in wanted]
    if compact is not None:
        # The compact query contains only requested native roots. Read only
        # direct imports for callers whose original strict action succeeded.
        query = audit.query("rust-ffi-configured-imports", "cquery",
                            "config(" + query_labels(imported) + ", target)")
        imports, omitted = read_cquery(query)
        if (omitted or {r["label"] for r in imports.values()} != set(imported)
                or any(r["rule_class"] != "cc_import" or r["is_tool"] for r in imports.values())):
            raise ValueError("Rust compact imports not the actual configured cc_import rules")
        rows.update(imports)
    options = ("--include_aspects", "--include_artifacts", "--include_commandline", "--include_param_files")
    scope = query_labels(sorted(wanted))
    sources = sorted({CONSUMERS[label][1] for label in imported})
    source_pattern = "^(" + "|".join(re.escape(source) for source in sources) + ")$"
    query = audit.query("rust-ffi-original-compile", "aquery",
                        "inputs('" + source_pattern + "', mnemonic('^CppCompile$', " + scope + "))", options)
    original = RuntimeActionGraph(query, "CppCompile", rows, NATIVE_CC)
    original_sha = sha_file(query)
    scan_scope = query_labels(sorted(wanted | set(imported)))
    query = audit.query("rust-ffi-original-dwyu", "aquery",
                        "mnemonic('^(" + "|".join(CPP_ACTIONS) + ")$', " + scan_scope + ")",
                        options + (aspect, "--output_groups=dwyu"))
    graph = RuntimeActionGraph(query, CPP_ACTIONS, rows, NATIVE_CC | {"cc_import"})
    queries = {"cpp": original_sha, "dwyu": sha_file(query)}
    observations = {}
    outputs = set()
    for owner, row in selected.items():
        for imported, record in row["strict"]["rust_imports"].items():
            record["queries"] = queries
            source = CONSUMERS[imported][1]
            try:
                compilers = [a for a in original.owners.get(owner, ()) if graph_source(original, a)[0] == source]
                reports = [a for a in graph.owners.get(owner, ()) if a["mnemonic"] == "DwyuRustImportEvidence"
                           and canonical(option(dwyu_arguments(a), "--import")) == imported]
                if len(compilers) != 1 or len(reports) != 1:
                    raise ValueError("FFI consumer CPP action or Rust observation missing or ambiguous")
                ev = reports[0]
                cmd = dwyu_arguments(ev)
                name = option(cmd, "--output")
                only_output(graph, graph.owners, name, owner, "DwyuRustImportEvidence")
                output = output_path(option(cmd, "--preprocessed"))
                trace = output_path(option(cmd, "--compiler-headers"))
                if (canonical(option(cmd, "--consumer")) != row["label"] or option(cmd, "--source") != source
                        or output == trace or not {output, trace} <= graph.inputs(ev)
                        or not re.fullmatch(r"[A-Za-z0-9_./@+=,~-]+", output)):
                    raise ValueError("Rust observation does not name this consumer's preprocessed source")
                outputs.add(output)
                observations[owner, imported] = compilers[0], ev, name
            except (KeyError, ValueError, TypeError, OSError) as exc:
                record["reason"] = str(exc)
    if not outputs:
        return
    # DBMS has thousands of other preprocessing actions. Only the explicitly
    # referenced outputs can supply a DwyuRustImportEvidence action above.
    pattern = "^(" + "|".join(re.escape(path) for path in sorted(outputs)) + ")$"
    query = audit.query("rust-ffi-preprocessing", "aquery",
                        "outputs('" + pattern + "', mnemonic('^DwyuCompilerPreprocess$', " + scope + "))",
                        options + (aspect, "--output_groups=dwyu"))
    preprocess = RuntimeActionGraph(query, "DwyuCompilerPreprocess", rows, NATIVE_CC)
    queries["preprocessing"] = sha_file(query)
    for (owner, imported), (cpp, ev, name) in observations.items():
        row = selected[owner]
        record = row["strict"]["rust_imports"][imported]
        try:
            if compact is not None:
                compact(owner, execroot / name)
            value, evidence = bep.read_report(execroot, owner, name)
            record.update(bind_cpp_observation(owner, imported, row, cpp, original, graph, preprocess, ev, value, rows))
            record["bep"] = evidence
        except (KeyError, ValueError, TypeError, OSError) as exc:
            record["status"] = "not_proved"
            record["reason"] = str(exc)

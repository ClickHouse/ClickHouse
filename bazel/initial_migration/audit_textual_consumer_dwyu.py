"""Check configured Boost, gRPC and msgpack textual headers in their existing compiled consumers.

This supplements an original DWYU owner report. It does not qualify a binary,
replace native link evidence, remove missing-dependency findings, or attest to
other owners by filename. The caller must retain the original report.
"""

import base64
import hashlib
import json
import re
from pathlib import Path

from audit_full_dwyu import (
    FIELDS, NATIVE_CC, RuntimeActionGraph, analyzer_arguments, analyzer_option,
    bep_file_matches, canonical, census_canonical, identity, query_labels, read_cquery, sha_file,
    validate_compiler_coverage, validate_link_evidence,
)
from audit_proto_dwyu import ProtoBep, expanded_args, graph_source, output_path
from audit_rust_import_dwyu import native_cpp_preprocessing


PACKAGE = "//bazel/initial_migration/dwyu_regressions/consumer:"
# The upstream boost_mysql_compiled target builds these two translation units.
BOOST = census_canonical("@clickhouse_boost//:")
BOOST_REPO = "external/" + BOOST.split("//", 1)[0].lstrip("@") + "/"
BOOST_FIXTURE = BOOST + "boost_mysql_compiled"
ASIO = BOOST_REPO + "libs/mysql/test/common/src/boost_asio.cpp"
MYSQL = BOOST_REPO + "libs/mysql/test/common/src/boost_mysql.cpp"
BOOST_SOURCES = {
    ASIO: "4dfd0d71f10febea5e85af27349a7ced24b70243381bdc444daa32087d424add",
    MYSQL: "dbdd6fb3d1fd132357a47353d9540ba61557134ce51c7855452ad0e44c3b5bb6",
}
# Each header contains an unconditional include inside its header guard.
BOOST_HEADERS = {
    "boost_separate_asio": {
        "boost/asio/impl/src.hpp": (ASIO, "boost/asio/impl/any_completion_executor.ipp"),
    },
    "boost_separate_openssl": {
        "boost/asio/ssl/impl/src.hpp": (ASIO, "boost/asio/ssl/impl/context.ipp"),
        "boost/mysql/src.hpp": (MYSQL, "boost/mysql/impl/any_connection.ipp"),
    },
}
BOOST_GROUPS = "--output_groups=dwyu_consumer,default,compilation_outputs"
CASES = {
    "boost_separate_asio": {"owner": BOOST + "boost_headers_no_openssl",
                           "consumer": BOOST_FIXTURE, "sources": tuple(BOOST_SOURCES)},
    "boost_separate_openssl": {"owner": BOOST + "boost_headers",
                              "consumer": BOOST_FIXTURE, "sources": tuple(BOOST_SOURCES)},
    "grpc_secure": {
        "owner": "@@clickhouse_grpc//:grpc++",
        "consumer": "//src/Server/grpc_protos:clickhouse_grpc_protos",
        "sources": (
            "bazel/gen/src/Server/grpc_protos/clickhouse_grpc.grpc.pb.cc",
            "bazel/gen/src/Server/grpc_protos/clickhouse_grpc.pb.cc",
        ),
    },
    "grpc_unsecure": {
        "owner": "@@clickhouse_grpc//:grpc++_unsecure",
        "consumer": PACKAGE + "generated_unsecure",
        "sources": (
            "bazel/gen/src/Server/grpc_protos/clickhouse_grpc.grpc.pb.cc",
            "bazel/gen/src/Server/grpc_protos/clickhouse_grpc.pb.cc",
        ),
    },
    "msgpack_formats": {
        "owner": "@@clickhouse_msgpack_c//:msgpack_c",
        "consumer": "//src:dbms__compile_0",
        "sources": (
            "src/Processors/Formats/Impl/MsgPackRowInputFormat.cpp",
            "src/Processors/Formats/Impl/MsgPackRowOutputFormat.cpp",
        ),
    },
}
LIMIT = 16 * 1024 * 1024


def matching_case(label):
    """Compare canonical repo names after this build's MODULE extension prefix."""
    label = canonical(label)
    for name, case in CASES.items():
        if name in BOOST_HEADERS:
            if label == case["owner"]:
                return name
            continue
        repo, tail = case["owner"].split("//", 1)
        wanted_repo = repo.lstrip("@")
        got_repo, got_tail = label.split("//", 1)
        got_repo = got_repo.lstrip("@")
        if got_tail == tail and (got_repo == wanted_repo or got_repo.endswith("+" + wanted_repo)):
            return name
    return None


def require(ok, message):
    if not ok:
        raise ValueError(message)


def bytes_record(raw):
    return {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def manifest_record(headers):
    return bytes_record(json.dumps(headers, sort_keys=True, separators=(",", ":")).encode())

def values(args, option):
    """Read the pinned make_param_file_args list format without losing order."""
    require(args.count(option) == 1, "missing or repeated analyzer option " + option)
    begin = args.index(option) + 1
    end = begin
    while end < len(args) and not args[end].startswith("--"):
        end += 1
    return args[begin:end]


def scoped_file(name, owner, path):
    """Only the original physical .inc/.hpp files of these three owners."""
    if matching_case(owner) != name or not isinstance(path, str):
        return False
    repo = canonical(owner).split("//", 1)[0].lstrip("@")
    prefix = "external/" + repo + "/"
    if not path.startswith(prefix):
        return False
    tail = path[len(prefix):]
    if name in BOOST_HEADERS:
        return tail in BOOST_HEADERS[name]
    if name in ("grpc_secure", "grpc_unsecure"):
        return tail in ("include/grpcpp/ports_def.inc", "include/grpcpp/ports_undef.inc")
    return bool(re.fullmatch(r"include/msgpack/preprocessor/iteration/detail/(bounds|iter)/[A-Za-z0-9_]+\.hpp", tail))


def read_named(bep, execroot, key, path, mnemonic, materialize):
    """Keep bytes only after this requested audit's current successful output."""
    output_path(path)
    require(key in bep.successful_roots and not bep.failed(key, path),
            "requested configured consumer action or root failed")
    reference = bep.named.get(path)
    require(isinstance(reference, dict), "consumer result is not a successful named output: " + path)
    if materialize is not None:
        materialize(reference, execroot / path, bep.invocation_uuid, path)
    raw, evidence = bep.read_file(execroot, key, path)
    require(0 < len(raw) <= LIMIT and bep_file_matches(reference, bytes_record(raw))
            and bep_file_matches(evidence["bep_file"], bytes_record(raw)),
            "consumer result absent, too large or changed from its BEP: " + path)
    return {"owner": key, "mnemonic": mnemonic, "output": path,
            "invocation_uuid": bep.invocation_uuid, "bep": bep.path,
            "stamp": bytes_record(raw), "named": reference, "data": base64.b64encode(raw).decode("ascii")}


def named_json(record, key, path, mnemonic, bep, invocation):
    """Reader-side digest and original configured output checks."""
    require(isinstance(record, dict) and record.get("owner") == key
            and record.get("mnemonic") == mnemonic and record.get("output") == path
            and record.get("bep") == bep and record.get("invocation_uuid") == invocation,
            "wrong configured consumer owner, invocation or output")
    raw = base64.b64decode(record["data"], validate=True)
    require(0 < len(raw) <= LIMIT and bytes_record(raw) == record.get("stamp")
            and str(Path(*record["named"].get("pathPrefix", [])) / record["named"]["name"]) == path
            and bep_file_matches(record["named"], record["stamp"]),
            "consumer bytes do not match the successful original named result")
    return json.loads(raw)


def short_source(path):
    # File.short_path of a generated file drops only bazel-out/<config>/bin.
    # Do not match basenames or suffixes from other repositories.
    parts = Path(path).parts
    if len(parts) >= 4 and parts[0] == "bazel-out" and parts[2] == "bin":
        return Path(*parts[3:]).as_posix()
    return path


def unique_output(graph, path, key, mnemonic):
    output_path(path)
    candidates = [(owner, action) for owner, actions in graph.owners.items()
                  for action in actions if path in graph.files(action, "outputIds")]
    require(len(candidates) == 1 and candidates[0][0] == key
            and candidates[0][1].get("mnemonic") == mnemonic
            and path not in graph.tree_artifacts,
            "missing, ambiguous or wrong owner of " + mnemonic + ": " + path)
    return candidates[0][1]


def filewrite(graph, path, key):
    action = unique_output(graph, path, key, "FileWrite")
    require(graph.files(action, "outputIds") == [path],
            "FileWrite has unexpected output artifacts")
    return action["fileContents"]


def case_rows(case, name, expected_key, original_row, rows):
    owner_candidates = [key for key, row in rows.items()
                        if matching_case(row["label"]) == name and not row["is_tool"]]
    audit_candidates = [key for key, row in rows.items()
                        if row["label"] == canonical(PACKAGE + name) and not row["is_tool"]]
    consumer_candidates = [key for key, row in rows.items()
                           if row["label"] == canonical(case["consumer"]) and not row["is_tool"]]
    require(owner_candidates == [expected_key] and len(audit_candidates) == len(consumer_candidates) == 1,
            "consumer audit does not reproduce the original configured owner")
    audit_key, consumer_key = audit_candidates[0], consumer_candidates[0]
    owner, audit, consumer = rows[expected_key], rows[audit_key], rows[consumer_key]
    require(owner["rule_class"] in NATIVE_CC and consumer["rule_class"] in NATIVE_CC
            and audit["rule_class"] == "dwyu_cc_consumer_audit",
            "consumer audit changed rule kinds")
    require(owner["checksum"] == original_row["checksum"] and
            audit["checksum"] == consumer["checksum"] == owner["checksum"],
            "owner and consumer audit have different target configurations")
    direct = {identity(item["label"], item["configurationChecksum"])
              for item in audit["configured_inputs"] if item.get("configurationChecksum")}
    require({expected_key, consumer_key} <= direct,
            "audit does not declare this configured owner and consumer")
    if name in BOOST_HEADERS:
        fixture_deps = {identity(i["label"], i["configurationChecksum"])
                        for i in consumer["configured_inputs"] if i.get("configurationChecksum")}
        require({identity(CASES[n]["owner"], owner["checksum"]) for n in BOOST_HEADERS} <= fixture_deps,
                "Boost fixture lacks its two direct configured owners")
        require(all([k for k, r in rows.items() if r["label"] == CASES[n]["owner"] and not r["is_tool"]]
                    == [identity(CASES[n]["owner"], owner["checksum"])] for n in BOOST_HEADERS),
                "Boost CcInfo owners differ from the original target configuration")
    return audit_key, consumer_key


def boost_query_sources():
    """Query only these three physical Files from both configured CcInfo owners."""
    owners = sorted(CASES[name]["owner"] for name in BOOST_HEADERS)
    query = "config(" + query_labels(owners) + ", target)\n"
    filenames = sorted(BOOST_REPO + h for headers in BOOST_HEADERS.values() for h in headers)
    formatter = """def format(target):
    p = providers(target)
    seen = [k for k in p if k.split("%")[-1] == "CcInfo"]
    if len(seen) != 1 or seen[0] not in ("@@rules_cc+//cc/private:cc_info.bzl%CcInfo", "@@rules_cc+0.2.26//cc/private:cc_info.bzl%CcInfo"):
        fail("Boost target lacks the pinned rules_cc CcInfo")
    c = p[seen[0]].compilation_context
    return json.encode([str(target.label), [
        [[f.path, f.short_path, f.is_directory] for f in files if f.path in EXACT_BOOST_FILES]
        for files in [c.direct_textual_headers, c.direct_public_headers, c.direct_private_headers]
    ]])
""".replace("EXACT_BOOST_FILES", repr(filenames))
    return query, formatter


def boost_header_query(audit):
    query, formatter = boost_query_sources()
    locations = [audit.out / ("textual-consumer-boost-headers." + ext) for ext in ("query", "bzl")]
    for file, code in zip(locations, (query, formatter)):
        file.write_text(code)
    run = audit.command("textual-consumer-boost-headers", "cquery",
        ["--query_file=" + str(locations[0]), "--output=starlark", "--consistent_labels",
         "--starlark:file=" + str(locations[1])], output=True)
    require(run["exit_code"] == 0, "Boost direct header CcInfo query failed")
    return {"run": run, "data": Path(run["stdout"]["file"]).read_text(),
            "inputs": [sha_file(f) for f in locations]}


def verify_boost_headers(evidence):
    run, data = evidence["run"], evidence["data"]
    inputs = evidence["inputs"]
    require(len(inputs) == 2 and
            all(bytes_record(code.encode()) == {f: rec[f] for f in ("sha256", "bytes")}
                for code, rec in zip(boost_query_sources(), inputs))
            and "--query_file=" + inputs[0]["file"] in run["command"]
            and "--starlark:file=" + inputs[1]["file"] in run["command"],
            "Boost CcInfo query or formatter changed")
    require(run["exit_code"] == 0 and "--output=starlark" in run["command"] and "--consistent_labels" in run["command"]
            and bytes_record(data.encode()) == {f: run["stdout"][f] for f in ("bytes", "sha256")},
            "changed Boost direct header CcInfo output")
    expected = {CASES[n]["owner"]: sorted((BOOST_REPO + p, "../" + (BOOST_REPO + p).removeprefix("external/"), False) for p in headers)
                for n, headers in BOOST_HEADERS.items()}
    for line in data.splitlines():
        label, (textual, modular, private) = json.loads(line)
        require(label in expected and sorted(tuple(f) for f in textual) == expected.pop(label)
                and not modular and not private, "wrong or duplicate configured Boost public textual File")
    require(not expected, "missing configured Boost public textual owner")


def boost_entries(name, owner, contexts, entered):
    """An existing file or a skipped guard does not execute any include inside src.hpp."""
    expected = {BOOST_REPO + path: (source, impl) for path, (source, impl) in BOOST_HEADERS[name].items()}
    require(owner == CASES[name]["owner"] and set(entered) == set(expected),
            "missing or foreign Boost textual ownership")
    seen = set()
    for context in contexts:
        for item in context["records"]:
            path = item["file"]
            if path not in expected:
                continue
            source, impl = expected[path]
            require(context["owner"] == owner and context["visibility"] == "public"
                    and context["source"] == source and path not in seen and item["entered"] is True,
                    "wrong Boost translation unit, owner, visibility, repeated or skipped header")
            require(item["resolved_includes"].count({"include": "<" + impl + ">", "file": BOOST_REPO + impl}) == 1,
                    "Clang did not observe the implementation include inside " + path)
            seen.add(path)
    require(seen == set(expected), "missing Boost implementation header body")


def boost_command(argv, source, obj):
    """Require the two upstream separate-compilation defines on the original command."""
    defines = {"BOOST_ASIO_SEPARATE_COMPILATION", "BOOST_MYSQL_SEPARATE_COMPILATION"}
    seen = []
    for i, arg in enumerate(argv):
        require(not arg.startswith(("-include", "--include", "-imacros", "-Wp,", "@")),
                "forced or opaque Boost preprocessor input")
        if arg in ("-D", "-U"):
            require(i + 1 < len(argv), "missing Boost macro argument")
            arg += argv[i + 1]
        if arg.startswith(("-D", "-U")):
            key = arg[2:].partition("=")[0]
            if key in defines:
                require(arg in ("-D" + key, "-D" + key + "=1"), "changed Boost separate-compilation define")
                seen.append(key)
            require(key not in ("BOOST_ASIO_HEADER_ONLY", "BOOST_MYSQL_HEADER_ONLY"), "Boost header-only command")
    require(sorted(seen) == sorted(defines) and argv.count("-c") == argv.count("-o") == 1
            and argv[argv.index("-c") + 1] == source and argv[argv.index("-o") + 1] == obj,
            "wrong Boost CppCompile source, object or definitions")


def boost_archive(graph, bep, key, compiled, root):
    """Bind the two configured compilation outputs to the actual built archive."""
    require(bep is not None and key in bep.successful_roots, "Boost fixture was not built")
    archive_actions = [a for a in graph.owners.get(key, ()) if a["mnemonic"] == "CppArchive"]
    require(len(archive_actions) == 1, "ambiguous or missing Boost CppArchive")
    action = archive_actions[0]
    files = graph.files(action, "outputIds")
    require(len(files) == 1 and unique_output(graph, files[0], key, "CppArchive") is action,
            "wrong Boost CppArchive output")
    archive = files[0]
    objects = {c["original_object"] for c in compiled}
    argv = expanded_args(action)
    require({x for x in graph.inputs(action) if x.endswith(".o")} == objects and
            sorted(x for x in argv if x.endswith(".o")) == sorted(objects) and argv.count(archive) == 1,
            "CppArchive did not use exactly the two Boost CppCompile outputs")
    for c in compiled:
        source, obj = c["source"], c["original_object"]
        cpp = unique_output(graph, obj, key, "CppCompile")
        require(graph_source(graph, cpp) == (source, obj), "borrowed Boost CppCompile")
        args = expanded_args(cpp)
        boost_command(args, source, obj)
        require(sha_file(root / source)["sha256"] == BOOST_SOURCES[source], "changed upstream Boost TU")
        c["boost_argv"] = args
        c["boost_source_sha256"] = BOOST_SOURCES[source]
    named = {}
    for path in sorted(objects | {archive}):
        require(not bep.failed(key, path) and path in bep.named, "missing native Boost output: " + path)
        named[path] = bep.named[path]
    return {"archive": archive, "inputs": sorted(objects), "archive_argv": argv, "named": named,
            "owner": key, "bep": bep.path, "invocation_uuid": bep.invocation_uuid}


def verify_boost_archive(native, compiled, consumer_key, bep, invocation):
    require(native["owner"] == consumer_key and native["bep"] == bep and native["invocation_uuid"] == invocation,
            "Boost archive belongs to another target, configuration or invocation")
    archive = output_path(native["archive"])
    expected = {c["original_object"] for c in compiled}
    fixtures = "external/" + BOOST.split("//", 1)[0].lstrip("@") + "/"
    parts = Path(archive).parts
    require(len(parts) == 6 and parts[2] == "bin" and "/".join(parts[3:]) == fixtures + "libboost_mysql_compiled.a"
            and len(compiled) == len(expected) == 2 and {c["source"] for c in compiled} == set(BOOST_SOURCES)
            and native["inputs"] == sorted(expected) and native["archive_argv"].count(archive) == 1
            and sorted(a for a in native["archive_argv"] if a.endswith(".o")) == sorted(expected),
            "Boost archive, configuration or original objects changed")
    for c in compiled:
        source, obj, args = c["source"], output_path(c["original_object"]), c["boost_argv"]
        require(c["consumer"] == consumer_key and c["boost_source_sha256"] == BOOST_SOURCES[source]
                and Path(obj).parts[:5] == parts[:5] and "/_objs/boost_mysql_compiled/" in obj
                and bytes_record(json.dumps(args).encode()) == c["cpp"], "unbound Boost CppCompile")
        boost_command(args, source, obj)
    require(set(native["named"]) == expected | {archive}, "Boost build did not name precisely these outputs")
    for path, item in native["named"].items():
        boost_output_digest(path, item)


def boost_output_digest(path, item):
    digest, size = item.get("digest"), item.get("length")
    if digest is None and size is None:
        blob = re.fullmatch(r"bytestream://[^/]+/(?:[^/]+/)*blobs/([0-9a-f]{64})/([1-9][0-9]*)",
                            item.get("uri", ""))
        require(blob, "missing Boost output digest")
        digest, size = blob.groups()
    require(str(Path(*item.get("pathPrefix", [])) / item["name"]) == path
            and re.fullmatch("[0-9a-f]{64}", digest) and str(size).isdigit() and int(size) > 0
            and bep_file_matches(item, {"sha256": digest, "bytes": int(size)}), "invalid Boost output digest")
    return digest, int(size)


def verify_boost_native_batch(ordinary_bep_file, manifest, checksum, receipts):
    """Join the two verified textual receipts to the ordinary batch's requested archive."""
    key = identity(BOOST_FIXTURE, checksum)
    native = ProtoBep(ordinary_bep_file, [BOOST_FIXTURE], {key: {}}, "default")
    require(native.successful_roots == {key} and set(receipts) == set(BOOST_HEADERS),
            "incomplete ordinary Boost build or missing textual owner")
    signatures = []
    for case, receipt in receipts.items():
        require(receipt["owner_status"] == "not_qualified" and receipt["cpp_qualified"] is False
                and receipt["audit"] == identity(PACKAGE + case, checksum) and receipt["consumer"] == key
                and receipt["boost_manifest"] == {k: manifest[k] for k in ("snapshot", "arch", "config")}
                and set(receipt["resolved_textual_files"]) == {BOOST_REPO + h for h in BOOST_HEADERS[case]},
                "wrong Boost snapshot, CPU, configured owner or header set")
        fixture = receipt["native_fixture"]
        archive = fixture["archive"]
        require(fixture["owner"] == key and archive in native.named and not native.failed(key, archive)
                and boost_output_digest(archive, native.named[archive]) ==
                boost_output_digest(archive, fixture["named"][archive]), "ordinary Boost archive does not match")
        signatures.append(sorted((p, boost_output_digest(p, f)) for p, f in fixture["named"].items()))
    require(signatures[0] == signatures[1], "Boost consumer builds compiled different objects or archives")
    return {"ordinary_invocation": native.invocation_uuid, "ordinary_bep": native.path,
            "archive": archive, "outputs": signatures[0], "cpp_qualified": False}



def check_report(original, observed, original_headers, new_headers, name=None, contexts=()):
    """Keep both reports. Credit only actual entries in named consumer output."""
    require(all(isinstance(original.get(field), kind) and isinstance(observed.get(field), kind)
                for field, kind in FIELDS.items()),
            "missing original or consumer report finding fields")
    require(canonical(original["analyzed_target"]) == canonical(observed["analyzed_target"]),
            "consumer analysis belongs to another owner")
    require(new_headers == original_headers,
            "consumer analysis uses changed physical owned headers or visibility")
    validate_compiler_coverage(original)
    validate_compiler_coverage(observed)
    validate_link_evidence(original)
    require(all(file in original_headers["required"] and
                file in original_headers[visibility] for file, visibility
                in original["unentered_owned_files"].items()),
            "original required file has no exact physical owner or changed visibility")
    require(all(file in original_headers["required"] and
                file in original_headers[visibility] for file, visibility
                in observed["unentered_owned_files"].items()),
            "consumer required file has no exact physical owner or changed visibility")
    remaining = observed["unentered_owned_files"]
    require(set(remaining) <= set(original["unentered_owned_files"]),
            "consumer analysis omitted observations already present in the original owner")
    entered = {}
    for context in contexts:
        require(context["owner"] == canonical(original["analyzed_target"])
                and context["visibility"] in ("public", "private"),
                "consumer context changes the owner or public/private side")
        visibility = context["visibility"]
        entries = context["records"]
        require(isinstance(entries, list), "consumer processor did not return per-file records")
        seen = set()
        for item in entries:
            require(isinstance(item, dict) and set(item) == {"file", "entered", "resolved_includes"}
                    and type(item["entered"]) is bool and isinstance(item["resolved_includes"], list),
                    "missing or malformed consumer preprocessing entry")
            path = item["file"]
            require(isinstance(path, str) and path not in seen, "duplicate consumer file record")
            seen.add(path)
            require(original_headers[visibility].get(path) == path,
                    "consumer JSON reports a foreign physical owner or wrong visibility")
            if (item["entered"] and original["unentered_owned_files"].get(path) == visibility
                    and path not in remaining and scoped_file(name, original["analyzed_target"], path)):
                entered.setdefault(path, []).append({
                    "visibility": visibility, "source": context["source"],
                    "output": context["output"], "stamp": context["stamp"],
                })
    if name in BOOST_HEADERS:
        boost_entries(name, original["analyzed_target"], contexts, entered)
    unresolved = {path: side for path, side in original["unentered_owned_files"].items() if path not in entered}
    original_findings = {field: len(original[field]) for field in FIELDS}
    new_findings = {field: len(observed[field]) for field in FIELDS}
    original_holds = {field: original.get(field) for field in (
        "unchecked_link_inputs", "unverified_link_deps", "unverified_compilation_deps",
        "unverified_compiler_inputs",
    )}
    new_holds = {field: observed.get(field) for field in (
        "unchecked_link_inputs", "unverified_link_deps", "unverified_compilation_deps",
        "unverified_compiler_inputs",
    )}
    return {
        "status": "scoped_textual_entries" if entered else "no_scoped_textual_entry",
        "entered_in_consumers": entered,
        "remaining": unresolved,
        "consumer_unentered": remaining,
        "original_findings": original_findings,
        "consumer_findings": new_findings,
        "original_holds": original_holds,
        "consumer_holds": new_holds,
        "owner_status": "not_qualified",
        "independent_owner_checks": [
            "original_strict_completion",
            "header_pp_coverage",
            "equivalent_owned_symbols_and_unused_dependencies",
        ],
        "cpp_qualified": False,
    }


def bind_graphs(name, key, row, source, headers, rows, action_graph, header_graph, pp_graph, original_graph,
                bep, execroot, materialize=None, native_bep=None):
    case = CASES[name]
    audit_key, consumer_key = case_rows(case, name, key, row, rows)
    require(audit_key in bep.successful_roots, "consumer result has no successful current BEP root")
    analyzers = [action for action in action_graph.owners.get(audit_key, ())
                 if action.get("mnemonic") == "DwyuAnalyzeOwnerWithConsumers"]
    require(len(analyzers) == 1, "missing or ambiguous consumer analyzer")
    analyzer = analyzers[0]
    argv = analyzer_arguments(analyzer)
    report_path = analyzer_option(argv, "--output")
    require(unique_output(action_graph, report_path, audit_key, "DwyuAnalyzeOwnerWithConsumers") is analyzer,
            "consumer report path differs")
    require(Path(report_path).name == name + "_dwyu_consumer_report.json",
            "consumer report is not the requested audit output")
    analyzer_public = values(argv, "--preprocessed_public_files")
    analyzer_private = values(argv, "--preprocessed_private_files")
    require(len(analyzer_public) == len(set(analyzer_public))
            and len(analyzer_private) == len(set(analyzer_private))
            and not set(analyzer_public).intersection(analyzer_private),
            "duplicate or overlapping analyzer public/private records")
    require(canonical(analyzer_option(argv, "--target_under_inspection")) == row["label"],
            "consumer analyzer uses the wrong owner label")
    owner_headers = analyzer_option(argv, "--owned_headers")
    require(owner_headers in action_graph.inputs(analyzer),
            "consumer analyzer lacks owned header input")
    new_headers = json.loads(filewrite(header_graph, owner_headers, key))
    require(new_headers == headers, "consumer CcInfo differs from original owner CcInfo")
    provenance = analyzer_option(argv, "--consumer_evidence")
    require(provenance in action_graph.inputs(analyzer),
            "consumer analyzer lacks evidence input")
    metadata = json.loads(filewrite(action_graph, provenance, audit_key))
    require(canonical(metadata["audit_target"]) == canonical(PACKAGE + name)
            and canonical(metadata["owner"]) == row["label"]
            and canonical(filewrite(header_graph, metadata["configured_owner"], key)) == row["label"],
            "configured owner proof differs from the original")
    consumers = metadata["consumers"]
    require(isinstance(consumers, list) and len(consumers) == 1
            and canonical(consumers[0]["consumer"]) == rows[consumer_key]["label"]
            and canonical(filewrite(header_graph, consumers[0]["configured_identity"], consumer_key)) ==
            rows[consumer_key]["label"], "consumer provenance has the wrong configured owner")
    sources = consumers[0]["sources"]
    require(isinstance(sources, list) and len(sources) == len(case["sources"])
            and {short_source(item["source"]) for item in sources} == set(case["sources"]),
            "requested real compiled sources differ from consumer evidence")
    checked = []
    contexts = []
    seen_pp = set()
    for item in sources:
        pp_name, compiler_source = item["preprocessed"], item["source"]
        require(pp_name not in seen_pp, "duplicated consumer preprocessing output")
        seen_pp.add(pp_name)
        pp = unique_output(pp_graph, pp_name, consumer_key, "DwyuCompilerPreprocess")
        require(compiler_source in pp_graph.inputs(pp),
                "original source is not a declared preprocessing input")
        trace = [f for f in pp_graph.files(pp, "outputIds") if f != pp_name]
        require(len(trace) == 1 and unique_output(pp_graph, trace[0], consumer_key, "DwyuCompilerPreprocess") is pp,
                "missing original compiler trace")
        original_compiles = [a for a in original_graph.owners.get(consumer_key, ())
                             if a["mnemonic"] == "CppCompile" and graph_source(original_graph, a)[0] == compiler_source]
        require(len(original_compiles) == 1,
                "consumer PP input is not a unique original CppCompile source")
        cpp = original_compiles[0]
        _, obj = graph_source(original_graph, cpp)
        manifests = [(owner, path) for owner, actions in header_graph.owners.items()
                     for a in actions if a["mnemonic"] == "FileWrite"
                     for path in header_graph.files(a, "outputIds")
                     if owner == consumer_key and path.endswith(".dwyu_owned_headers.json")
                     and path in pp_graph.inputs(pp)]
        require(len(manifests) == 1, "consumer preprocessing has no single configured header input")
        native_cpp_preprocessing(cpp, original_graph, pp, pp_graph,
                                 compiler_source, obj, pp_name, trace[0], manifests[0][1])
        processors = [a for a in action_graph.owners.get(audit_key, ())
                      if a["mnemonic"] == "DwyuProcessConsumerContext"
                      and analyzer_option(analyzer_arguments(a), "--source") == compiler_source]
        require(len(processors) == 1, "compiler result was not reprocessed once for this owner")
        processor = processors[0]
        pargs = analyzer_arguments(processor)
        public = analyzer_option(pargs, "--output")
        private = analyzer_option(pargs, "--private_output")
        require(analyzer_option(pargs, "--files") == pp_name
                and analyzer_option(pargs, "--compiler_headers") == trace[0]
                and analyzer_option(pargs, "--owned_headers") == owner_headers
                and analyzer_option(pargs, "--mode") == "compiler"
                and pargs.count("--consumer_root") == 1
                and {pp_name, trace[0], owner_headers, compiler_source} <= action_graph.inputs(processor)
                and set(action_graph.files(processor, "outputIds")) == {public, private}
                and unique_output(action_graph, public, audit_key, "DwyuProcessConsumerContext") is processor
                and unique_output(action_graph, private, audit_key, "DwyuProcessConsumerContext") is processor
                and public in analyzer_public and private in analyzer_private
                and {public, private} <= action_graph.inputs(analyzer),
                "consumer processor does not bind the original trace and physical owner")
        actual_pp = expanded_args(pp)
        command = json.loads(analyzer_option(pargs, "--compiler_command"))
        replay = ["-c" if arg == "-E" else arg for arg in actual_pp[9:-8]]
        require(isinstance(command, list) and command == replay,
                "consumer processor uses different search flags than the real original CPP/PP")
        require(isinstance(cpp.get("actionKey"), str) and bool(cpp["actionKey"]),
                "original CppCompile action key missing")
        records = {}
        for visibility, output in (("public", public), ("private", private)):
            evidence = read_named(bep, execroot, audit_key, output, "DwyuProcessConsumerContext", materialize)
            entries = named_json(evidence, audit_key, output, "DwyuProcessConsumerContext",
                                 bep.path, bep.invocation_uuid)
            records[visibility] = evidence
            contexts.append({"owner": row["label"], "visibility": visibility, "source": compiler_source,
                             "output": output, "stamp": evidence["stamp"], "records": entries})
        checked.append({
            "source": compiler_source, "original_object": obj, "preprocessed": pp_name,
            "trace": trace[0], "cpp_action_key": cpp["actionKey"], "owner": key, "consumer": consumer_key,
            "cpp": bytes_record(json.dumps(expanded_args(cpp)).encode()),
            "pp": bytes_record(json.dumps(actual_pp).encode()),
            "processor": bytes_record(json.dumps(pargs).encode()),
            "records": records,
        })
    report_evidence = read_named(bep, execroot, audit_key, report_path,
                                 "DwyuAnalyzeOwnerWithConsumers", materialize)
    observed = named_json(report_evidence, audit_key, report_path, "DwyuAnalyzeOwnerWithConsumers",
                          bep.path, bep.invocation_uuid)
    require(observed.get("consumer_evidence") == metadata, "consumer report has changed provenance")
    result = check_report(source, observed, headers, new_headers, name, contexts)
    result.update({"audit": audit_key, "owner": key, "consumer": consumer_key,
                   "original_compilations": checked, "consumer_report": report_evidence,
                   "case": name, "schema": 2, "owner_manifest": manifest_record(new_headers),
                   "provenance": metadata})
    if name in BOOST_HEADERS:
        result["native_fixture"] = boost_archive(original_graph, native_bep, consumer_key, checked, execroot)
        verify_boost_archive(result["native_fixture"], checked, consumer_key, bep.path, bep.invocation_uuid)
    return result


def collect(audit, selected, original_headers, execroot, *, group=None, manifest=None, materialize=None):
    """Attach evidence only. Never replace the original strict/ordinary record."""
    compact = group is not None
    if compact:
        require(isinstance(manifest, dict) and manifest.get("phase") == "candidate"
                and manifest.get("arch") in ("arm", "x86")
                and re.fullmatch("[0-9a-f]{40}", manifest.get("snapshot", ""))
                and manifest.get("config") == audit.args.config
                and group.get("rows") is selected and group.get("strict_bep") and callable(materialize),
                "not a source-bound current compact CPP group")
    else:
        require(manifest is None and materialize is None, "missing compact CPP group")
    candidates = {}
    for key, row in selected.items():
        name = matching_case(row["label"])
        expected_ordinary = "not_tested" if compact else "passed"
        if (name is not None and row["ordinary"]["status"] == expected_ordinary
                and row["strict"]["status"] == "unentered_owned_files"
                and len(original_headers.get(key, [])) == 1):
            require(key == identity(row["label"], row["checksum"])
                    and not row["is_tool"] and row["rule_class"] in NATIVE_CC,
                    "changed original configured owner")
            candidates[key] = name
    if not candidates:
        return
    audit_labels = sorted({PACKAGE + name for name in candidates.values()})
    consumer_labels = sorted({CASES[name]["consumer"] for name in candidates.values()})
    fixture = [BOOST_FIXTURE] if set(candidates.values()) & BOOST_HEADERS.keys() else []
    labels = audit_labels + consumer_labels + [selected[key]["label"] for key in candidates]
    if fixture:
        labels += [CASES[name]["owner"] for name in BOOST_HEADERS]
    configuration_file = audit.query(
        "textual-consumer-configurations", "cquery", "config(" + query_labels(labels) + ", target)")
    rows, omitted = read_cquery(configuration_file)
    require(not omitted, "unconfigured files in consumer query")
    options = ("--include_artifacts", "--include_aspects", "--include_commandline", "--include_param_files")
    audit_scope = query_labels(audit_labels)
    graph_file = audit.query("textual-consumer-analyzers", "aquery",
        "mnemonic('^(DwyuAnalyzeOwnerWithConsumers|DwyuProcessConsumerContext|FileWrite)$', " + audit_scope + ")",
        options + ("--include_file_write_contents", "--output_groups=dwyu_consumer"))
    graph = RuntimeActionGraph(graph_file,
        ("DwyuAnalyzeOwnerWithConsumers", "DwyuProcessConsumerContext", "FileWrite"), None, None)
    head_file = audit.query("textual-consumer-header-identities", "aquery",
        "outputs('.*([.]dwyu_configured_owner|[.]dwyu_owned_headers[.]json)$', deps(" + audit_scope + "))",
        options + ("--include_file_write_contents", "--output_groups=dwyu_consumer"))
    head = RuntimeActionGraph(head_file, "FileWrite", None, None)
    # These are complete relative short paths, never just a basename.
    source_regex = "(^|bazel-out/[^/]+/bin/)(" + "|".join(re.escape(s)
        for s in sorted({s for name in candidates.values() for s in CASES[name]["sources"]})) + ")$"
    pp_file = audit.query("textual-consumer-preprocessing", "aquery",
        "inputs('" + source_regex + "', mnemonic('^DwyuCompilerPreprocess$', deps(" + audit_scope + ")))",
        options + ("--output_groups=dwyu_consumer",))
    pp = RuntimeActionGraph(pp_file, "DwyuCompilerPreprocess", None, None)
    cpp_query = "inputs('" + source_regex + "', mnemonic('^CppCompile$', " + query_labels(consumer_labels) + "))"
    if fixture:
        cpp_query += " union mnemonic('^CppArchive$', " + query_labels(fixture) + ")"
    cpp_file = audit.query("textual-consumer-original-cpp", "aquery", cpp_query, options)
    cpp = RuntimeActionGraph(cpp_file, ("CppCompile", "CppArchive"), None, None)
    bep_path = audit.out / "textual-consumer.bep.json"
    run = audit.command("textual-consumer", "build", [
        "--keep_going", BOOST_GROUPS if fixture else "--output_groups=dwyu_consumer",
        "--remote_download_outputs=minimal" if compact else "--remote_download_outputs=toplevel",
        "--build_event_publish_all_actions", "--build_event_json_file=" + str(bep_path)] + audit_labels + fixture)
    native_bep = ProtoBep(bep_path, fixture, rows) if fixture else None
    bep = ProtoBep(bep_path, audit_labels, rows, "dwyu_consumer")
    evidence = {"configurations": sha_file(configuration_file),
                "analyzers": sha_file(graph_file), "headers": sha_file(head_file),
                "preprocessing": sha_file(pp_file), "cpp": sha_file(cpp_file),
                "run": run, "bep": bep.path, "invocation_uuid": bep.invocation_uuid,
                "manifest": manifest,
                "selected": {key: candidates[key] for key in sorted(candidates)}}
    if fixture:
        evidence["boost_headers"] = boost_header_query(audit)
        verify_boost_headers(evidence["boost_headers"])
    for key, name in candidates.items():
        row = selected[key]
        try:
            raw = Path(row["strict"]["report"]["file"]).read_bytes()
            previous = row["strict"]["report"]
            require(len(raw) == previous["bytes"] and hashlib.sha256(raw).hexdigest() == previous["sha256"],
                    "original CPP owner report changed")
            if compact:
                require(raw == base64.b64decode(group["reports"][key], validate=True),
                        "compact original report differs from owned source")
            record = bind_graphs(name, key, row, json.loads(raw),
                                 original_headers[key][0]["contents"], rows, graph, head, pp, cpp, bep, execroot,
                                 materialize, native_bep)
        except (KeyError, TypeError, ValueError, RuntimeError, OSError) as exc:
            record = {"status": "not_proved", "reason": str(exc), "cpp_qualified": False}
        record["queries"] = evidence
        row["strict"]["textual_consumer_evidence"] = record


def verify_compact(group, key, row, parsed, manifest):
    """Verify textual consumption only; keep every original owner check."""
    name = matching_case(row["label"])
    require(name is not None and key == identity(row["label"], row["checksum"])
            and row["ordinary"]["status"] == "not_tested" and not row["is_tool"]
            and row["rule_class"] in NATIVE_CC and row["strict"]["status"] == "unentered_owned_files"
            and parsed["cpp_qualified"] is False and parsed["identity"] == key,
            "textual receipt cannot qualify an original owner")
    record = row["strict"]["textual_consumer_evidence"]
    require(record.get("schema") == 2 and record.get("case") == name and record.get("owner") == key
            and record.get("cpp_qualified") is False and record.get("owner_status") == "not_qualified",
            "missing configured textual collector evidence")
    queries = record["queries"]
    require(queries.get("manifest") == manifest and manifest.get("phase") == "candidate"
            and manifest.get("arch") in ("arm", "x86") and queries["selected"].get(key) == name,
            "consumer receipt is from a different source/configuration/CPU group")
    checksum = row["checksum"]
    audit_key = identity(PACKAGE + name, checksum)
    consumer_key = identity(CASES[name]["consumer"], checksum)
    require(record["audit"] == audit_key and record["consumer"] == consumer_key,
            "consumer or audit was not in the owner's original target configuration")
    original_ref = row["strict"]["report"]
    raw = base64.b64decode(group["reports"][key], validate=True)
    require(len(raw) <= LIMIT and parsed["original"] == original_ref
            and bytes_record(raw) == {field: original_ref[field] for field in ("sha256", "bytes")},
            "original owner report changed or is unavailable")
    original = json.loads(raw)
    headers = row["strict"]["configured_headers"]["contents"]
    require(record["owner_manifest"] == manifest_record(headers),
            "consumer required physical ownership/visibility differs from original")
    for field, suffix, command in (
        ("configurations", "configurations", "cquery"), ("analyzers", "analyzers", "aquery"),
        ("headers", "header-identities", "aquery"), ("preprocessing", "preprocessing", "aquery"),
        ("cpp", "original-cpp", "aquery"),
    ):
        runs = [r for r in group["runs"] if r["name"] == "textual-consumer-" + suffix]
        require(len(runs) == 1 and runs[0]["exit_code"] == 0 and runs[0]["command"].count(command) == 1
                and runs[0]["stdout"] == queries[field]
                and "--consistent_labels" in runs[0]["command"],
                "changed or failed original consumer metadata query: " + suffix)
    builds = [r for r in group["runs"] if r["name"] == "textual-consumer"]
    downloads = [arg for arg in builds[0]["command"] if arg.startswith("--remote_download_outputs=")] if len(builds) == 1 else []
    require(len(builds) == 1 and builds[0] == queries["run"]
            and builds[0]["command"].count("build") == 1
            and "--keep_going" in builds[0]["command"]
            and (BOOST_GROUPS if set(queries["selected"].values()) & BOOST_HEADERS.keys()
                 else "--output_groups=dwyu_consumer") in builds[0]["command"]
            and "--build_event_publish_all_actions" in builds[0]["command"]
            and downloads and set(downloads) == {"--remote_download_outputs=minimal"},
            "missing original minimal consumer build")
    bep, invocation = queries["bep"], queries["invocation_uuid"]
    require("--build_event_json_file=" + bep["file"] in builds[0]["command"] and invocation,
            "consumer output not bound to this actual BEP")
    rpt = record["consumer_report"]
    require(Path(rpt["output"]).name == name + "_dwyu_consumer_report.json",
            "unexpected consumer report filename")
    observed = named_json(rpt, audit_key, rpt["output"], "DwyuAnalyzeOwnerWithConsumers", bep, invocation)
    require(observed.get("consumer_evidence") == record["provenance"]
            and canonical(record["provenance"]["owner"]) == row["label"]
            and canonical(record["provenance"]["audit_target"]) == canonical(PACKAGE + name),
            "consumer report changed configured provenance")
    compilations = record["original_compilations"]
    require(len(compilations) == len(CASES[name]["sources"])
            and {short_source(c["source"]) for c in compilations} == set(CASES[name]["sources"]),
            "not the requested genuine consumer translation units")
    if name in BOOST_HEADERS:
        runs = [r for r in group["runs"] if r["name"] == "textual-consumer-boost-headers"]
        require(runs == [queries["boost_headers"]["run"]], "Boost CcInfo belongs to another invocation")
        require("--config=" + manifest["config"] in builds[0]["command"]
                and "--config=" + manifest["config"] in runs[0]["command"], "wrong Boost build configuration")
        verify_boost_headers(queries["boost_headers"])
        configured = record["provenance"]["consumers"]
        require(len(configured) == 1 and canonical(configured[0]["consumer"]) == BOOST_FIXTURE
                and sorted((r["source"], r["preprocessed"]) for r in configured[0]["sources"])
                == sorted((r["source"], r["preprocessed"]) for r in compilations),
                "Boost CppCompile or preprocessing belongs to another configured translation unit")
        require(builds[0]["command"].count(BOOST_FIXTURE) == 1, "Boost was not an ordinary requested target")
        verify_boost_archive(record["native_fixture"], compilations, consumer_key, bep, invocation)
    contexts, seen_outputs = [], set()
    for cpp in compilations:
        require(cpp["owner"] == key and cpp["consumer"] == consumer_key
                and isinstance(cpp["cpp_action_key"], str) and cpp["cpp_action_key"]
                and all(isinstance(cpp[field], dict)
                        and re.fullmatch("[0-9a-f]{64}", cpp[field].get("sha256", ""))
                        and type(cpp[field].get("bytes")) is int and cpp[field]["bytes"] > 0
                        for field in ("cpp", "pp", "processor")),
                "missing original source CPP/PP/processor action evidence")
        require(set(cpp["records"]) == {"public", "private"}, "missing consumer processor visibility")
        for side, item in cpp["records"].items():
            path = output_path(item["output"])
            require(path not in seen_outputs and path.endswith("_" + side + ".json"),
                    "duplicate consumer processor or swapped public/private JSON")
            seen_outputs.add(path)
            entries = named_json(item, audit_key, path, "DwyuProcessConsumerContext", bep, invocation)
            contexts.append({"owner": row["label"], "visibility": side, "source": cpp["source"],
                             "output": path, "stamp": item["stamp"], "records": entries})
    result = check_report(original, observed, headers, headers, name, contexts)
    require(all(record.get(field) == value for field, value in result.items()),
            "textual consumption/remaining/dependency/hold summary differs from its original reports")
    # Findings may be summarized here, but remain complete on parsed. This is
    # evidence about files only: it cannot discharge any whole-owner checks.
    return {
        "resolved_textual_files": result["entered_in_consumers"],
        "unresolved_textual_files": result["remaining"],
        "original_findings": result["original_findings"], "consumer_findings": result["consumer_findings"],
        "original_holds": result["original_holds"], "consumer_holds": result["consumer_holds"],
        "independent_owner_checks": result["independent_owner_checks"],
        "owner_status": "not_qualified", "cpp_qualified": False,
        "audit": audit_key, "consumer": consumer_key, "report": rpt["stamp"],
        "original": original_ref, "bep": bep, "invocation_uuid": invocation,
        **({"native_fixture": record["native_fixture"],
            "boost_manifest": {f: manifest[f] for f in ("snapshot", "arch", "config")}}
           if name in BOOST_HEADERS else {}),
    }

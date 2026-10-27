#!/usr/bin/env python3
"""Generate native Bazel target data from both configured CMake File API graphs."""

import ast
import collections
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from initial_migration.codegen import (
    Codegen, common_googleapis_rules, google_api_schemas, googleapis_path, googleapis_source_map, render_rules,
)
from initial_migration.cmake_metadata import read_metadata, write_metadata
from initial_migration.rust_archives import ARCHIVE_TARGETS as RUST_ARCHIVES
from initial_migration.reference_readers import read_file_api
from initial_migration.configuration import read_configurations
from initial_migration import llvm_targets
from initial_migration.arrow_inputs import foreign_arrow_dependencies
from initial_migration.external_headers import HeaderLibraries
from initial_migration.package_codegen import PACKAGES as PACKAGE_CODEGEN_DIRECTORIES
from initial_migration.format_build_files import format_build_files
from initial_migration.measured_deps import FUNCTION_REGISTRATION
from initial_migration.prune_inherited_headers import prune, prune_published
from initial_migration.prune_unused_exports import prune_native_exports, prune_unused_exports
from initial_migration.build_file import (
    BuildFile, CMakeCxxCompilerVersion, Glob,
    SOURCE_HEADER_DIRECTORIES, SOURCE_HEADER_SUFFIXES, SOURCE_HEADER_TARGETS,
    EMBEDDED_SOURCES, dependency_inputs, embedded_indices, relocate_compilation,
    relocate_tools, select, starlark, target_label, target_owner,
)
from initial_migration.repository_inputs import (
    REPOSITORIES, cmake_has_source_prefix_map, comparison_root, configuration_owner, execution_path, repository,
    CC_HEADER_SUFFIXES, HeaderDirectory, directory_sources, input_key,
    package_sources, source_label, source_path, write_overlays,
)

ROOT = Path(__file__).resolve().parents[2]
ARCHES = ('x86_64', 'aarch64')
COMPILE_TYPES = {'STATIC_LIBRARY', 'OBJECT_LIBRARY', 'EXECUTABLE'}
HEADER_SUFFIXES = {'.h', '.hh', '.hpp', '.hxx', '.inc', '.ipp', '.inl', '.tcc', '.def'}
BCR_DEPENDENCIES = json.loads((ROOT / 'bazel/initial_migration/bcr_dependencies.json').read_text())
LICENSES_OUTPUT = 'src/Storages/System/StorageSystemLicenses.generated.cpp'


# Headers read by ClickHouse source files in compiler dependency reports.
# Public headers use deps so consumers receive the same CcInfo.
SYSTEM_TABLE_REGISTRATION = "//src/Storages/System:clickhouse_storages_system__compile_1"
CLIENT_PARSER_HEADER_DEPS = ["//src/Common:string_utils", "@clickhouse_boost//:boost_headers_no_openssl"]
# A (dependency, predecessor) pair keeps C++ include directories and linker inputs in order.
AGGREGATE_HEADERS = ("//src/AggregateFunctions:headers", "//src/AggregateFunctions:clickhouse_aggregate_functions__compile_0")
FIRST_PARTY_HEADER_DEPS = {
    ("src", "dbms", "deps"): [(dep, ":dbms__compile_0") for dep in CLIENT_PARSER_HEADER_DEPS],
    ("src", "dbms__compile_2", "implementation_deps"): [
        ("//src/AggregateFunctions:aggregate_api", "//src/AggregateFunctions:headers"),
        ("//src/Common:cache_policy", "//src/Common"),
    ],
    ("src", "clickhouse_common_io__compile_0", "deps"): [
        ("//base/poco/NetSSL_OpenSSL:_poco_net_ssl__compile_0", "//base/poco/NetSSL_OpenSSL:_poco_net_ssl"),
    ],
    ("src", "clickhouse_common_io__compile_0", "implementation_deps"): [
        "//src/Parsers:parser_api", "//src/Parsers:clickhouse_parsers",
    ],
    ("src", "dbms__compile_0", "deps"): [
        "@clickhouse_sqlite_amalgamation//:_sqlite", "//contrib/cityhash102:_cityhash",
        "//src/Access/Common:headers", "@clickhouse_google_protobuf//:_libprotobuf",
        "@clickhouse_mongo_cxx_driver//:_mongocxx", "//src/AggregateFunctions:aggregate_api",
        "//src/Parsers:clickhouse_parsers",
    ],
    ("src", "unit_tests_dbms", "deps"): [
        ("//src/AggregateFunctions:headers", "//src/AggregateFunctions:clickhouse_aggregate_functions"),
        "//src/AggregateFunctions:clickhouse_aggregate_functions__compile_0",
        "//src/Dictionaries:clickhouse_dictionaries__compile_0",
        "@clickhouse_abseil_cpp//:absl_synchronization", "@clickhouse_arrow//:_arrow__compile_0",
        "@clickhouse_aws//:_aws__compile_0", "@lz4//:lz4",
    ],
    ("programs/benchmark", "clickhouse-benchmark-lib", "implementation_deps"): [
        ("//src/AggregateFunctions:clickhouse_aggregate_functions", "//programs/client:clickhouse-client-lib"),
        "//src/Parsers:parser_api", AGGREGATE_HEADERS,
        ("//src:dbms", "//programs/client:clickhouse-client-lib"),
        ("@clickhouse_boost//:_boost_program_options", "//programs/client:clickhouse-client-lib"),
    ],
    ("programs/client", "clickhouse-client-lib", "implementation_deps"): [
        "//src/Parsers:headers", FUNCTION_REGISTRATION, AGGREGATE_HEADERS,
    ],
    ("programs/format", "clickhouse-format-lib", "implementation_deps"): [
        "//src/Parsers:parser_api", "//src/Functions:headers", "//src/Parsers:headers", FUNCTION_REGISTRATION, AGGREGATE_HEADERS,
    ],
    ("programs/install", "clickhouse-install-lib", "implementation_deps"): ["//src/Access/Common:headers"],
    ("programs/keeper-bench", "clickhouse-keeper-bench-lib", "implementation_deps"): [AGGREGATE_HEADERS],
    ("programs/keeper-client", "clickhouse-keeper-client-lib", "implementation_deps"): ["//src:dbms__compile_0"],
    **{("programs/" + name, f"clickhouse-{name}-lib", "implementation_deps"): ["//src:headers"]
        for name in ("keeper-converter", "keeper-utils", "packed-io")},
    ("programs/local", "clickhouse-local-lib", "implementation_deps"): [
        ("//src/AggregateFunctions:clickhouse_aggregate_functions", "//programs/server:clickhouse-server-lib"),
        "//src/Functions:headers", "//src/Parsers:headers", FUNCTION_REGISTRATION, SYSTEM_TABLE_REGISTRATION, AGGREGATE_HEADERS,
    ],
    ("programs/server", "clickhouse-server-lib", "implementation_deps"): [
        "//src/Functions:headers", "//src/Access/Common:headers", "//src/Parsers:headers",
        FUNCTION_REGISTRATION, SYSTEM_TABLE_REGISTRATION, AGGREGATE_HEADERS,
    ],
    ("src/Common/ZooKeeper", "clickhouse_common_zookeeper_base", "implementation_deps"): ["//src/Parsers:headers"],
    ("src/Common/ZooKeeper", "headers", "deps"): ["//src:column_types"],
    ("src/Compression", "clickhouse_compression", "implementation_deps"): ["//src/Parsers:headers"],
    ("src/Dictionaries", "clickhouse_dictionaries__compile_1", "implementation_deps"): ["//src:dbms__compile_0"],
    ("src/Dictionaries", "headers", "deps"): [
        "//bazel:clickhouse_config_headers", "@clickhouse_boost//:boost_headers_no_openssl",
    ],
    ("src/Functions", "clickhouse_functions_obj__compile_0", "implementation_deps"): [
        ":headers", "//src/Access/Common:headers", "//src/Functions/GatherUtils:headers", "//src:dbms__compile_0",
    ],
    ("src/Functions/GatherUtils", "clickhouse_functions_gatherutils", "implementation_deps"): ["//src:headers"],
    ("src/Functions/TimeSeries", "clickhouse_functions_time_series", "implementation_deps"): ["//src:headers"],
    ("src/Functions/UniqTheta", "clickhouse_functions_uniqtheta", "implementation_deps"): [
        "//src/Functions:headers", "//src:headers", AGGREGATE_HEADERS,
    ],
    ("src/Functions/URL", "clickhouse_functions_url__compile_0", "implementation_deps"): ["//src/Functions:headers"],
    ("src/Functions/array", "array_distance_transposed", "implementation_deps"): ["//src/Functions:headers"],
    ("src/Functions/array", "clickhouse_functions_array__compile_0", "implementation_deps"): [
        "//src/Functions:headers", "//src/Functions/GatherUtils:headers",
    ],
    ("src/Functions/array", "clickhouse_functions_array__compile_1", "implementation_deps"): ["//src:column_types"],
    ("src/Loggers", "loggers", "deps"): ["//src/Common:parser_support_api"],
    ("src/Parsers", "clickhouse_parsers", "deps"): [
        "//bazel:clickhouse_config_headers", "//src/Common:parser_support_api", "//src/Common:memory_tracking_api",
    ],
    ("src/Storages/benchmarks", "benchmark_statistics_objects", "implementation_deps"): [
        ("//src/AggregateFunctions:headers", "//src:dbms"),
    ],
    ("src/Storages/System", "clickhouse_storages_system__compile_1", "implementation_deps"): ["//src/Access/Common:headers"],
    ("src/Storages/System", "clickhouse_storages_system__compile_1", "deps"): [
        "//bazel:clickhouse_config_headers", "//src:with_context",
    ],
    ("src/Storages/System", "clickhouse_storages_system_information_schema", "implementation_deps"): ["//src/Parsers:parser_api"],
    ("src/TableFunctions", "clickhouse_table_functions", "implementation_deps"): ["//src/Access/Common:headers"],
}


def declare_boost_header_libraries(build):
    """CMake describes the compiled Boost libraries. The template declares the installed headers."""
    from initial_migration.compact_build import Source

    contents = Path(__file__).with_name("boost_headers.bazel.tmpl").read_text()
    for _, call in Source(contents).rules():
        name = next(kw.value.value for kw in call.keywords if kw.arg == "name")
        if name in build.names:
            raise ValueError(f"Duplicate Boost header library {name}")
        build.names.add(name)
    build.loads.setdefault("@rules_cc//cc:cc_library.bzl", set()).add("cc_library")
    build.rules.append(contents)


def declare_external_compiler_headers(root):
    """Publish Abseil's optional_ref.h with Status and keep CRoaring's forced include private."""
    from initial_migration.compact_build import Source

    overlays = root / "bazel/dependencies"
    for repository, target, field, header, old, replacement in (
        ("clickhouse_abseil_cpp", "absl_status", "hdrs", "absl/types/optional_ref.h",
         ":absl_internal_optional_ref", ":absl_hardening"),
        ("clickhouse_croaring", "_roaring", "srcs",
         "@clickhouse//contrib/croaring-cmake:page_aligned_functions.h",
         "@clickhouse//contrib/croaring-cmake:page_aligned_functions", None),
    ):
        path = overlays / (repository + ".BUILD.bazel")
        source = Source(path.read_text())
        rules = {next(k.value.value for k in rule.keywords if k.arg == "name"): (stmt, rule)
                 for stmt, rule in source.rules()}
        rule = rules[target][1]
        attrs = {k.arg: k.value for k in rule.keywords}
        edits = []
        items = attrs.get(field)
        if items is None:
            pos = source.span(rule)[1] - 1
            separator = "" if source.bytes[:pos].rstrip().endswith(b",") else ","
            edits.append((pos, pos, separator + f"\n    {field} = {json.dumps([header])},\n"))
        elif not any(isinstance(n, ast.Constant) and n.value == header for n in ast.walk(items)):
            values = source.text(items) + " + " + json.dumps([header])
            if isinstance(items, ast.List):
                values = json.dumps(ast.literal_eval(items) + [header])
            edits.append((*source.span(items), values))
        if old.startswith(":") and old[1:] in rules:
            statement, donor = rules[old[1:]]
            donor_attrs = {k.arg: k.value for k in donor.keywords}
            if ast.literal_eval(donor_attrs["hdrs"]) != [header]:
                raise ValueError(f"{old} now owns other headers")
            edits.append((*source.span(statement), ""))
        # The original CMake import records direct -include files in hdrs.
        # After package splitting the same file was a header library instead.
        for field_name in ("deps", "implementation_deps", "hdrs", "textual_hdrs", "additional_compiler_inputs"):
            if field_name == field:
                continue
            deps = attrs.get(field_name)
            if deps is None:
                continue
            for group in (n for n in ast.walk(deps) if isinstance(n, ast.List)):
                retired = {old} if replacement else {old, old.rsplit(":", 1)[0] + ":headers", header}
                if not any(isinstance(n, ast.Constant) and n.value in retired for n in group.elts):
                    continue
                values = [replacement if v in retired else v for v in ast.literal_eval(group)]
                edits.append((*source.span(group), json.dumps(list(dict.fromkeys(v for v in values if v)))))
        if edits:
            path.write_text(source.rewrite(edits))

    # Only CRoaring's roaring.c uses -include on this one ClickHouse source file.
    path = root / "contrib/croaring-cmake/BUILD.bazel"
    source = Source(path.read_text())
    allowed = {"page_aligned_functions.h"}
    if (any(next(k.value.value for k in rule.keywords if k.arg == "name")
            not in {"headers", "page_aligned_functions"} for _, rule in source.rules())
            or any(set(ast.literal_eval(n.args[0])) - allowed for n in ast.walk(source.module)
                   if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "exports_files")):
        raise ValueError(f"{path} contains other targets")
    path.write_text('exports_files(\n    ["page_aligned_functions.h"],\n'
                    '    visibility = ["@clickhouse_croaring//:__pkg__"],\n)\n')




def append_header_names(document, node, headers):
    tail = node.right if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add) else node
    if isinstance(tail, ast.List):
        return (*document.source.span(tail), json.dumps([*ast.literal_eval(tail), *headers]))
    end = document.source.span(node)[1]
    return end, end, " + " + json.dumps(headers)


def header_glob_exclusion(target):
    hdrs = next(kw.value for kw in target.keywords if kw.arg == "hdrs")
    glob, = [n for n in ast.walk(hdrs) if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "glob"]
    return glob, next((kw.value for kw in glob.keywords if kw.arg == "exclude"), None)


def exclude_common_headers(document, target, headers):
    glob, exclude = header_glob_exclusion(target)
    if exclude is not None:
        return append_header_names(document, exclude, headers)
    end = document.source.span(glob)[1] - 1
    return end, end, ", exclude = " + json.dumps(headers)


def declare_int_exp(root, common, rules):
    """Move the CMake intExp.cpp member together with the two headers that declare its tables."""
    from initial_migration.prune_inherited_headers import Document

    src = Document(root / "src/BUILD.bazel", package="src")
    library = rules(common).get("int_exp")
    owner = rules(src)["clickhouse_common_io__compile_0"]
    srcs = next(kw.value for kw in owner.keywords if kw.arg == "srcs")
    inputs = [n for n in ast.walk(srcs) if getattr(n, "value", None) == "//src/Common:intExp.cpp"]
    if library is not None:
        if inputs:
            raise ValueError("intExp.cpp already has two compilation owners")
        return
    if len(inputs) != 1:
        raise ValueError("expected one intExp.cpp in clickhouse_common_io__compile_0")

    def remove(source, node):
        start, end = source.span(node)
        return start, end + (source.bytes[end:end + 1] == b","), ""

    src.path.write_text(src.source.rewrite([remove(src.source, inputs[0])]))
    edits = []
    headers = ["exp10_i32.h", "intExp.h"]
    owners = rules(common)
    aggregate = owners.get("Common") or owners["headers"]
    edits.append(exclude_common_headers(common, aggregate, headers))
    if parser_headers := common.constants.get("_PARSER_SUPPORT_HEADERS"):
        edits.extend(remove(common.source, n) for n in parser_headers.elts if n.value == "exp10_i32.h")
    for stmt in common.source.module.body:
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call) and getattr(stmt.value.func, "id", None) == "exports_files":
            edits.extend(remove(common.source, n) for n in ast.walk(stmt.value.args[0])
                         if getattr(n, "value", None) == "intExp.cpp")
    loads = {arg.value for st in common.source.module.body if isinstance(st, ast.Expr) and isinstance(st.value, ast.Call)
             and getattr(st.value.func, "id", None) == "load" for arg in st.value.args[1:] if isinstance(arg, ast.Constant)}
    for symbol, filename in (("ch_cc_library", "@clickhouse//bazel/rules:ch_cc_library.bzl"),
                             ("CLICKHOUSE_COMMON_IO_LOCAL_DEFINES", "@clickhouse//src:common_io_flags.bzl")):
        if symbol not in loads:
            edits.append((0, 0, f'load("{filename}", "{symbol}")\n'))
    common.path.write_text(common.source.rewrite(edits) + """
ch_cc_library(
    name = "int_exp",
    srcs = ["intExp.cpp"],
    hdrs = ["exp10_i32.h", "intExp.h"],
    clickhouse_code = True,
    local_defines = CLICKHOUSE_COMMON_IO_LOCAL_DEFINES,
    strip_include_prefix = "/src",
    deps = ["//base:common"],
)
""")


def declare_throttler_headers(root, common, rules):
    """IThrottler.h is public; only dbms translation units include ThrottlerArray.h."""
    from initial_migration.prune_inherited_headers import Document

    owners = rules(common)
    common_name = "Common" if "Common" in owners else "headers"
    if "throttler_interface" not in owners:
        private = common.constants.get("_PRIVATE_DBMS_HEADERS")
        headers = ["IThrottler.h"] if private is not None else ["IThrottler.h", "ThrottlerArray.h"]
        edits = [exclude_common_headers(common, owners[common_name], headers)]
        extra = ""
        if private is None:
            extra = '\nexports_files(["ThrottlerArray.h"], visibility = ["//src:__pkg__"])\n'
        else:
            edits.append(append_header_names(common, private, ["ThrottlerArray.h"]))
        common.path.write_text(common.source.rewrite(edits) + extra + """
ch_cc_library(
    name = "throttler_interface",
    hdrs = ["IThrottler.h"],
    features = ["skip_virtual_includes"],
    strip_include_prefix = "/src",
    deps = [
        "//base:common",
        "//base/poco/Net:_poco_net",
        "@clickhouse_boost//:boost_headers_no_openssl",
    ],
)
""")
        src = Document(root / "src/BUILD.bazel", package="src")
        fields = {kw.arg: kw.value for kw in rules(src)["dbms__compile_0"].keywords}
        private = src.constants.get("_DBMS_0_PRIVATE_SRC_HEADERS", fields["srcs"])
        src.path.write_text(src.source.rewrite([append_header_names(src, private, ["//src/Common:ThrottlerArray.h"])]))
    return common_name


def declare_common_header_owners(root, rules):
    """Keep Common headers with their memory, parser, and string libraries."""
    from initial_migration.prune_inherited_headers import Document

    common = Document(root / "src/Common/BUILD.bazel", package="src/Common")
    declare_int_exp(root, common, rules)
    common = Document(root / "src/Common/BUILD.bazel", package="src/Common")
    common_name = declare_throttler_headers(root, common, rules)
    common = Document(root / "src/Common/BUILD.bazel", package="src/Common")
    owners = rules(common)
    if "once_and_atomic" not in owners:
        headers = ["CopyableAtomic.h", "callOnce.h"]
        exclude = exclude_common_headers(common, owners[common_name], headers)
        common.path.write_text(common.source.rewrite([exclude]) + f"""
ch_cc_library(
    name = "once_and_atomic",
    hdrs = {json.dumps(headers)},
    clickhouse_code = True,
    features = ["skip_virtual_includes"],
    strip_include_prefix = "/src",
)
""")
        common = Document(common.path, package="src/Common")
        owners = rules(common)
    # These public headers and translation units include intExp.h or exp10_i32.h directly.
    edges = {
        (package, name, field): ["//src/Common:int_exp"] for package, name, field in (
            ("src", "headers", "deps"),
            ("src/Functions", "headers", "deps"),
            ("src", "clickhouse_common_io__compile_0", "implementation_deps"),
            ("src", "dbms__compile_0", "implementation_deps"),
            ("src/Functions", "clickhouse_functions_obj__compile_0", "implementation_deps"),
        )
    }
    for package, name, field in (
        ("src/Common", common_name, "deps"),
        ("src", "headers", "deps"),
        ("src", "clickhouse_common_io__compile_0", "deps"),
        ("src", "dbms__compile_0", "implementation_deps"),
    ):
        label = ":throttler_interface" if package == "src/Common" else "//src/Common:throttler_interface"
        edges.setdefault((package, name, field), []).append(label)
    for name, field in (("headers", "deps"), ("dbms__compile_0", "implementation_deps")):
        edges["src", name, field].append("//src/Common:once_and_atomic")
    edits = []

    def add(source, items, name, anchor, *, before=False):
        names = [item.value for item in items.elts]
        if name not in names:
            a, b = source.span(items.elts[names.index(anchor)])
            edits.append((a, a, json.dumps(name) + ", ") if before else (b, b, ", " + json.dumps(name)))

    def attrs(rule):
        return {kw.arg: kw.value for kw in rule.keywords}

    if "string_utils" in owners:
        exclusions = attrs(attrs(owners["Common"])["hdrs"])["exclude"]
        named = next(n for n in ast.walk(exclusions) if isinstance(n, ast.List)
                     and any(getattr(item, "value", None) == "StringUtils.h" for item in n.elts))
        for items in (attrs(owners["string_utils"])["hdrs"], named):
            anchor = next(item.value for item in items.elts
                          if item.value in ("StringHashForHeterogeneousLookup.h", "StringUtils.h"))
            add(common.source, items, "HTTPFieldLess.h", anchor, before=True)
            add(common.source, items, "StringHashForHeterogeneousLookup.h", "StringUtils.h", before=True)
        edges["src/Common/ZooKeeper", "headers", "deps"] = ["//src/Common:string_utils"]

    if "memory_tracking_api" in owners:
        headers = common.constants["_MEMORY_TRACKING_API_HEADERS"]
        names = {item.value for item in headers.elts}
        public = names | {
            "DevectorWithMemoryTracking.h", "ProcessorMemoryStats.h",
            "QueueWithMemoryTracking.h", "StringWithMemoryTracking.h",
        }
        if public != names:
            edits.append((*common.source.span(headers), json.dumps(sorted(public))))
        edges["src/Common", "memory_tracking_api", "deps"] = ["@clickhouse_boost//:boost_headers_no_openssl"]

    if "parser_support_api" in owners:
        headers = common.constants["_PARSER_SUPPORT_HEADERS"]
        add(common.source, headers, "DateLUTImpl.h", "DateLUT.h")
        add(common.source, headers, "LocalDate.h", "Logger.h", before=True)
        add(common.source, headers, "LocalDateTime.h", "Logger.h", before=True)
        add(common.source, headers, "LocalTime.h", "Logger.h", before=True)
        add(common.source, headers, "Logger_fwd.h", "Logger.h")
        add(common.source, headers, "ShellCommandSettings.h", "SipHash.h", before=True)
        parser = "//src/Common:parser_support_api"
        for package, target, field in (
            ("programs/client", "clickhouse-client-lib", "implementation_deps"),
            ("src", "dbms__compile_1", "implementation_deps"),
            ("src/Functions", "headers", "deps"),
        ):
            edges.setdefault((package, target, field), []).append(parser)

    if edits:
        common.path.write_text(common.source.rewrite(edits))

    if "parser_support_api" in owners:
        # Only x86_64 compiles StorageGenerateRandom.cpp in dbms__compile_3.
        dbms = Document(root / "src/BUILD.bazel", package="src")
        targets = rules(dbms)
        options = attrs(targets["dbms__compile_3"])
        impl = options["implementation_deps"]
        providers = [parser]
        if "memory_tracking_api" in owners:
            providers.append("//src/Common:memory_tracking_api")
        if "io_string_buffers" in targets:
            providers.append("//src:io_string_buffers")
        labels = {cpu: dbms.labels(impl, cpu) | dbms.labels(options.get("deps"), cpu) for cpu in ARCHES}
        if any(dbms.reference(dep) in labels["aarch64"] for dep in providers):
            raise ValueError("dbms__compile_3 uses the StorageGenerateRandom.cpp header dependencies only on x86_64")
        missing = [dep for dep in providers if dbms.reference(dep) not in labels["x86_64"]]
        if missing:
            choices = impl.right.args[0]
            x86 = next(v for k, v in zip(choices.keys, choices.values) if k.value == "@platforms//cpu:x86_64")
            end = dbms.source.span(x86.elts[-1])[1]
            dbms.path.write_text(dbms.source.rewrite([(end, end, ", " + ", ".join(map(json.dumps, missing)))]))
    edges["src", "clickhouse_common_io__compile_0", "deps"].extend([
        "//src/Common:current_metrics", "//src/Common:cache_policy",
    ])
    return edges


def declare_group_header_deps(root, package, paired_headers, providers, public_owner, public_target=None):
    """Declare each source's private header and the direct dependencies used to compile both."""
    from initial_migration.factor_build_selections import canonical_list, list_nodes
    from initial_migration.prune_inherited_headers import ARCHES, Document

    path = root / package / "BUILD.bazel"
    doc = Document(path, package=package, files=list(paired_headers.values()))
    source = doc.source
    edits = []

    def append(node, labels):
        return canonical_list(ast.BinOp(node, ast.Add(), ast.List([ast.Constant(label) for label in labels], ast.Load())))

    for _, call in source.rules():
        fields = {kw.arg: kw.value for kw in call.keywords}
        name = fields["name"].value
        if name == "headers":
            _, excluded = header_glob_exclusion(call)
            missing = [h for h in paired_headers.values() if h not in doc.values(excluded, ARCHES[0])]
            if missing:
                edits.append(exclude_common_headers(doc, call, missing))
        if name == public_target:
            hdrs = fields.get("hdrs")
            missing = [h for h in paired_headers.values() if h not in doc.values(hdrs, ARCHES[0])]
            if missing:
                if hdrs is None:
                    end = source.span(fields["name"])[1]
                    edits.append((end, end, ", hdrs = " + json.dumps(missing)))
                else:
                    edits.append(append_header_names(doc, hdrs, missing))
        srcs = fields.get("srcs")
        if srcs is None or not any(isinstance(n, ast.Constant) and n.value in providers for n in ast.walk(srcs)):
            continue
        deps = fields["implementation_deps"]
        for items in (n for n in list_nodes(srcs) if isinstance(n, ast.List)):
            files = [item.value for item in items.elts if isinstance(item, ast.Constant)]
            missing = [h for cpp, h in paired_headers.items() if cpp in files and h not in files]
            if missing:
                edits.append((*source.span(items), ast.unparse(append(items, missing))))
        additions = {}
        for cpu in ARCHES:
            inputs = {f.removeprefix(":") for f in doc.values(srcs, cpu)}
            direct = doc.labels(deps, cpu) | doc.labels(fields.get("deps"), cpu)
            owners = (public_owner(dep) for cpp, labels in providers.items() if cpp in inputs for dep in labels)
            additions["@platforms//cpu:" + cpu] = list(dict.fromkeys(dep for dep in owners if doc.reference(dep) not in direct))
        if not any(additions.values()):
            continue
        last = deps.right if isinstance(deps, ast.BinOp) else deps
        if (isinstance(last, ast.Call) and getattr(last.func, "id", None) == "select"
                and set(additions) <= {key.value for key in last.args[0].keys}):
            choices = last.args[0]
            for key, row in zip(choices.keys, choices.values):
                if additions.get(key.value):
                    edits.append((*source.span(row), ast.unparse(append(row, additions[key.value]))))
        else:
            common = os.path.commonprefix(list(additions.values()))
            value = ast.unparse(append(deps, common)) if common else source.text(deps)
            remaining = {cpu: labels[len(common):] for cpu, labels in additions.items()}
            if any(remaining.values()):
                value += " + select(" + json.dumps(remaining) + ")"
            edits.append((*source.span(deps), value))
    if edits:
        path.write_text(source.rewrite(edits))


def privatize_headerless_deps(root, targets, *, alwayslink=False):
    """Keep the link order of deps + implementation_deps for libraries without public headers."""
    from initial_migration.compact_build import Source
    from initial_migration.factor_build_selections import canonical_list

    for package, target in targets:
        path = root / package / "BUILD.bazel"
        source = Source(path.read_text())
        rule, = [call for _, call in source.rules()
                 if any(kw.arg == "name" and getattr(kw.value, "value", None) == target for kw in call.keywords)]
        fields = {kw.arg: kw for kw in rule.keywords}
        if rule.func.id != "ch_cc_library":
            raise ValueError(f"//{package}:{target} is no longer a ch_cc_library")
        if {"hdrs", "textual_hdrs"} & fields.keys():
            raise ValueError(f"//{package}:{target} publishes headers; review its public dependencies")
        if alwayslink and ("alwayslink" not in fields or ast.literal_eval(fields["alwayslink"].value) is not True):
            raise ValueError(f"//{package}:{target} no longer links all its registration objects")
        if public := fields.get("deps"):
            value, edits = public.value, []
            if private := fields.get("implementation_deps"):
                value = canonical_list(ast.BinOp(value, ast.Add(), private.value))
                edits.append((source.starts[private.lineno - 1], source.starts[private.end_lineno], ""))
            edits.append((*source.span(public), "implementation_deps = " + ast.unparse(value)))
            path.write_text(source.rewrite(edits))


def declare_first_party_header_deps(root):
    from initial_migration.measured_deps import editable_dependency_lists
    from initial_migration.prune_inherited_headers import ARCHES, Document

    def rules(doc):
        return {name.value: call for _, call in doc.source.rules()
                for kw in call.keywords if kw.arg == "name" and isinstance((name := kw.value), ast.Constant)}

    def public_owner(label):
        # The initial CMake packages own these headers before the compiled libraries are split out.
        headers = {
            "//src/Common:parser_support_api": ["BitHelpers.h", "DateLUT.h", "NaNUtils.h", "PODArray.h", "SipHash.h", "iota.h"],
            "//src/AggregateFunctions:aggregate_api": ["IAggregateFunction.h", "IAggregateFunction_fwd.h"],
            "//src/Common:cache_policy": ["CacheBase.h"],
            "//src/Common:current_metrics": ["CurrentMetrics.h"],
            "//src/Common:exception_api": ["Exception.h", "typeid_cast.h"],
            "//src/Common:memory_tracking_api": ["VectorWithMemoryTracking.h"],
            "//src/Common:target_specific": ["TargetSpecific.h"],
            "//src:core_defines": ["Core/Defines.h"],
            "//src:column_types": [
                "Columns/ColumnArray.h", "Columns/ColumnConst.h", "Columns/ColumnFixedString.h", "Columns/ColumnMap.h",
                "Columns/ColumnNullable.h", "Columns/ColumnTuple.h",
            ],
            "//src:with_context": ["Interpreters/Context_fwd.h"],
        }.get(label)
        if headers is None:
            return label
        package, name = label[2:].split(":")
        doc = Document(root / package / "BUILD.bazel", package=package, files=headers)
        owners = rules(doc)
        if name in owners:
            return label
        attrs = {kw.arg: kw.value for kw in owners["headers"].keywords}
        for cpu in ARCHES:
            if not {doc.reference(":" + h) for h in headers} <= doc.labels(attrs["hdrs"], cpu):
                raise ValueError(f"//{package}:headers does not declare {headers} on {cpu}")
        return f"//{package}:headers"

    def common_list(node):
        if isinstance(node, ast.List):
            return node
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return common_list(node.right) or common_list(node.left)
        return None

    declare_group_header_deps(root, "src/Functions", {"pointInPolygon.cpp": "PolygonUtils.h"}, {
        # PolygonUtils.h is private here, so its own includes need direct providers too.
        "pointInPolygon.cpp": [
            ":headers", "//src/Common:parser_support_api", "//src/Common:memory_tracking_api",
            "//src/Common:cache_policy", "//src:core_defines", "//src/Common:exception_api",
        ],
        "FunctionsHashingMisc.cpp": [
            ":headers", "//src/Common:parser_support_api", "//src/Common:memory_tracking_api", "//src/Common:target_specific",
        ],
    }, public_owner)
    parser = "Client/ClientApplicationBaseParser.cpp"
    declare_group_header_deps(root, "src", {parser: parser.removesuffix(".cpp") + ".h"},
                              {parser: CLIENT_PARSER_HEADER_DEPS}, public_owner, public_target="dbms")
    for (package, target, field), requirements in (*FIRST_PARTY_HEADER_DEPS.items(), *declare_common_header_owners(root, rules).items()):
        deps, after = [], {}
        for requirement in requirements:
            label, predecessor = requirement if isinstance(requirement, tuple) else (requirement, None)
            dep = public_owner(label)
            deps.append(dep)
            if predecessor is not None:
                after[dep] = predecessor
        path = root / package / "BUILD.bazel"
        if (package, target) == ("src/Functions/TimeSeries", "clickhouse_functions_time_series"):
            privatize_headerless_deps(root, [(package, target)], alwayslink=True)
        doc = Document(path, package=package)
        attrs = {kw.arg: kw.value for kw in rules(doc)[target].keywords}
        if field == "deps" and (package, target) in {
            ("src/Parsers", "clickhouse_parsers"),
            ("src", "dbms__compile_0"),
            ("src", "clickhouse_common_io__compile_0"),
            ("src/Storages/System", "clickhouse_storages_system__compile_1"),
        }:
            # Remove only this compiler's private references, including shared dict attributes.
            private = attrs.get("implementation_deps")
            if private is not None:
                public = {doc.reference(dep) for dep in deps}
                edits = editable_dependency_lists(doc, private, public, doc.reference)
                if edits is None:
                    raise ValueError(f"cannot promote the private dependencies of //{package}:{target}")
                if edits:
                    path.write_text(doc.source.rewrite(edits))
                    doc = Document(path, package=package)
                    attrs = {kw.arg: kw.value for kw in rules(doc)[target].keywords}
        node = attrs.get(field)
        other = attrs.get("deps" if field == "implementation_deps" else "implementation_deps")
        missing = []
        for label in deps:
            owner = doc.reference(label)
            if any(owner in doc.labels(other, cpu) for cpu in ARCHES):
                if field == "implementation_deps" and all(owner in doc.labels(other, cpu) for cpu in ARCHES):
                    continue  # A public dependency already supplies this private include.
                raise ValueError(f"//{package}:{target} has {label} in the wrong attribute")
            present = [owner in doc.labels(node, cpu) for cpu in ARCHES]
            if not any(present):
                missing.append(label)
            elif not all(present):
                raise ValueError(f"//{package}:{target} has an architecture-specific {field} for {label}")
        if not missing:
            continue
        literal = common_list(node)
        if literal is not None and package == "programs/server" and "//src/Access/Common:headers" in missing:
            parser = next((x for x in literal.elts if getattr(x, "value", None) == "//src/Parsers:headers"), None)
            if parser is not None:
                at = doc.source.span(parser)[0]
                path.write_text(doc.source.rewrite([(at, at, '"//src/Access/Common:headers", ')]))
                missing.remove("//src/Access/Common:headers")
                if not missing:
                    continue
                doc = Document(path, package=package)
                node = next(kw.value for kw in rules(doc)[target].keywords if kw.arg == field)
                literal = common_list(node)
        if literal is not None and package.startswith("programs/") and missing in (
                ["//src:headers"], ["//src:dbms__compile_0"]):
            # Keep these direct providers before //src:dbms to preserve the order of shared LinkerInputs.
            dbms = next((item for item in literal.elts if getattr(item, "value", None) == "//src:dbms"), None)
            if dbms is not None:
                start = doc.source.span(dbms)[0]
                path.write_text(doc.source.rewrite([(start, start, json.dumps(missing[0]) + ", ")]))
                continue
        if len(missing) == 1 and missing[0] in after:
            dep = missing[0]
            edits = editable_dependency_lists(doc, node, set(), doc.reference, insert_after=(after[dep], [dep]))
            if not edits:
                raise ValueError(f"//{package}:{target} cannot insert {dep} after {after[dep]} in {field}")
            path.write_text(doc.source.rewrite(edits))
            continue
        if after.keys() & set(missing):
            if literal is None:
                raise ValueError(f"//{package}:{target} has no common list for the ordered {field}")
            values = ast.literal_eval(literal)
            for dep in reversed(missing):
                if dep in after:
                    values.insert(values.index(after[dep]) + 1, dep)
            values.extend(dep for dep in missing if dep not in after)
            edit = (*doc.source.span(literal), json.dumps(values))
        elif literal is not None:
            start, end = doc.source.span(literal)
            text = doc.source.text(literal)[:-1].rstrip()
            separator = "," if literal.elts and not text.endswith(",") else ""
            edit = (start, end, text + separator + "\n" + ",\n".join(map(json.dumps, missing)) + ",\n]")
        elif node is not None:
            end = doc.source.span(node)[1]
            edit = (end, end, " + " + json.dumps(missing))
        else:
            end = doc.source.span(attrs["name"])[1]
            edit = (end, end, f",\n    {field} = " + json.dumps(missing))
        path.write_text(doc.source.rewrite([edit]))


def declare_compiled_parser_headers(path):
    """Publish these parser declarations on the library that compiles their implementations."""
    from bisect import insort
    from initial_migration.prune_inherited_headers import ARCHES, Document

    headers = [
        "ASTAlterQuery.h", "ASTAssignment.h", "ASTAsterisk.h", "ASTCheckDatabaseQuery.h",
        "ASTCheckQuery.h", "ASTColumnsMatcher.h",
        "ASTColumnsTransformers.h", "ASTConstraintDeclaration.h", "ASTCreateFunctionWithDriverQuery.h",
        "ASTCreateHandlerQuery.h", "ASTCreateIndexQuery.h", "ASTCreateWasmFunctionQuery.h",
        "ASTDatabaseOrNone.h", "ASTDeleteQuery.h",
        "ASTDropFunctionQuery.h", "ASTDropHandlerQuery.h", "ASTDropIndexQuery.h", "ASTDropQuery.h",
        "ASTDropResourceQuery.h", "ASTDropWorkloadQuery.h", "ASTEnumDataType.h",
        "ASTFromJSON.h", "ASTHypotheticalObjectQuery.h", "ASTIndexDeclaration.h", "ASTInsertQuery.h",
        "ASTInterpolateElement.h", "ASTKillQueryQuery.h",
        "ASTOptimizeQuery.h", "ASTOrderByElement.h", "ASTParallelWithQuery.h", "ASTPartition.h",
        "ASTProjectionDeclaration.h", "ASTProjectionSelectQuery.h", "ASTQualifiedAsterisk.h",
        "ASTRenameQuery.h",
        "ASTSelectIntersectExceptQuery.h", "ASTSelectWithUnionQuery.h", "ASTShowColumnsQuery.h",
        "ASTShowFunctionsQuery.h",
        "ASTShowIndexesQuery.h", "ASTShowTablesQuery.h", "ASTSnapshotQuery.h",
        "ASTStatisticsDeclaration.h", "ASTStreamSettings.h", "ASTSubquery.h", "ASTSystemQuery.h",
        "ASTTTLElement.h", "ASTTablesInSelectQuery.h", "ASTTransactionControl.h", "ASTTupleDataType.h",
        "ASTUndropQuery.h",
        "ASTUpdateQuery.h", "ASTUseQuery.h", "ASTWindowDefinition.h", "ASTWithElement.h",
        "Access/ASTCreateQuotaQuery.h", "Access/ASTCreateRoleQuery.h", "Access/ASTCreateRowPolicyQuery.h",
        "Access/ASTCreateSettingsProfileQuery.h", "Access/ASTCreateUserQuery.h",
        "Access/ASTDropAccessEntityQuery.h", "Access/ASTExecuteAsQuery.h", "Access/ASTGrantQuery.h",
        "Access/ASTPublicSSHKey.h", "Access/ASTRolesOrUsersSet.h", "Access/ASTSetRoleQuery.h",
        "Access/ASTSettingsProfileElement.h",
        "Access/ParserCreateUserQuery.h", "Access/ParserGrantQuery.h", "CreateQueryUUIDs.h",
        "LiteralTokenInfo.h", "ParserAlterQuery.h", "ParserAttachAccessEntity.h",
        "ParserCreateFunctionQuery.h", "ParserCreateHandlerQuery.h", "ParserCreateResourceQuery.h",
        "ParserCreateWorkloadEntity.h", "ParserCreateWorkloadQuery.h", "ParserDropResourceQuery.h",
        "ParserDropWorkloadQuery.h", "ParserOptimizeQuery.h", "ParserPreparedStatement.h",
        "ParserRegistry.h", "ParserSelectWithUnionQuery.h", "QueryParameterVisitor.h",
        "TablePropertiesQueriesASTs.h", "makeASTForLogicalFunction.h", "obfuscateQueries.h",
        "parseIdentifierOrStringLiteral.h",
        "queryNormalization.h", "registerParsers.h",
        "Kusto/KQLLexer.h", "Prometheus/parseTimeSeriesTypes.h",
    ]
    def attrs(doc, name):
        return next({kw.arg: kw.value for kw in call.keywords} for _, call in doc.source.rules()
                    if any(kw.arg == "name" and kw.value.value == name for kw in call.keywords))

    doc = Document(path, package="src/Parsers", files=headers)
    compiler = attrs(doc, "clickhouse_parsers")
    published = compiler.get("hdrs")
    missing = []
    for header in headers:
        present = [doc.reference(":" + header) in doc.labels(published, arch) for arch in ARCHES]
        if any(present) != all(present):
            raise ValueError(f"clickhouse_parsers.hdrs differs between CPUs for {header}")
        if not any(present):
            missing.append(header)
    if missing:
        if isinstance(published, ast.Name) and isinstance(items := doc.constants.get(published.id), ast.List):
            values = ast.literal_eval(items)
            for header in missing:
                insort(values, header)
            edit = (*doc.source.span(items), json.dumps(values))
        elif published is not None:
            end = doc.source.span(published)[1]
            edit = (end, end, " + " + json.dumps(missing))
        else:
            end = doc.source.span(compiler["name"])[1]
            edit = (end, end, ",\n    hdrs = " + json.dumps(missing))
        doc = Document(path, text=doc.source.rewrite([edit]), package="src/Parsers", files=headers)

    shared = attrs(doc, "headers")["hdrs"]
    if not isinstance(shared, ast.Call) or getattr(shared.func, "id", None) != "glob":
        raise ValueError("expected src/Parsers:headers.hdrs to be a glob")
    excluded = next((kw.value for kw in shared.keywords if kw.arg == "exclude"), None)
    missing = [header for header in headers if not all(header in doc.values(excluded, arch) for arch in ARCHES)]
    if missing:
        end = doc.source.span((shared.keywords or shared.args)[-1] if excluded is None else excluded)[1]
        insertion = ", exclude = " if excluded is None else " + "
        doc = Document(path, text=doc.source.rewrite([(end, end, insertion + json.dumps(missing))]),
                       package="src/Parsers", files=headers)
    path.write_text(doc.source.bytes.decode("utf-8"))


def declare_unit_test_private_hdrs(path):
    """Declare the implementation files included by unit_tests_dbms and their private headers."""
    from initial_migration.compact_build import Source

    source = Source(path.read_text())
    target = next(rule for _, rule in source.rules()
                  if any(kw.arg == 'name' and kw.value.value == 'unit_tests_dbms' for kw in rule.keywords))
    attrs = {kw.arg: kw for kw in target.keywords}
    merge_tree_headers = [
        'Storages/MergeTree/MergeTreeDataPartCompact.h',
        'Storages/MergeTree/MergeTreeDataPartWriterCompact.h',
        'Storages/MergeTree/MergeTreeDataPartWriterWide.h',
        'Storages/MergeTree/MergeTreeReaderIndex.h',
        'Storages/MergeTree/ParallelSyncFiles.h',
        'Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h',
    ]
    if 'private_hdrs' in attrs:
        value = attrs['private_hdrs'].value
        common = value.left if isinstance(value, ast.BinOp) and isinstance(value.op, ast.Add) else value
        if not isinstance(common, ast.List):
            raise ValueError('unit_tests_dbms private_hdrs must list common files before select()')
        files = ast.literal_eval(common)
        if missing := [header for header in merge_tree_headers if header not in files]:
            path.write_text(source.rewrite([(*source.span(common), repr(files + missing))]))
        return
    inputs = attrs['additional_compiler_inputs']
    files = ast.literal_eval(inputs.value)
    shared = [
        'Access/AccessRights.cpp',
        '//src/Common:Base58.cpp',
        '//src/Common:Hex.cpp',
        '//src/Functions:FunctionMD5.cpp',
        '//src/Functions:FunctionsStringDistance.cpp',
        'Storages/MergeTree/MergeTreeDataPartWriterCompact.cpp',
        'Storages/MergeTree/MergeTreeDataPartWriterWide.cpp',
        'Storages/MergeTree/MergeTreeRangeReader.cpp',
    ]
    sha1 = '//src/Functions:FunctionSHA1.cpp'
    included = set(shared) | {sha1}
    if missing := included - set(files):
        raise ValueError(f'unit_tests_dbms no longer lists included implementation files: {sorted(missing)}')
    other = [file for file in files if file not in included]
    replacement = f'additional_compiler_inputs = {other!r},\n    ' if other else ''
    replacement += f'private_hdrs = {shared + merge_tree_headers!r} + select({{"@platforms//cpu:x86_64": [{sha1!r}], "//conditions:default": []}})'
    path.write_text(source.rewrite([(*source.span(inputs), replacement)]))



def llvm_source_overlays():
    """Read ClickHouse source files supplied to the LLVM module extension."""
    tree = ast.parse((ROOT / 'bazel/llvm.MODULE.bazel').read_text())
    for stmt in tree.body:
        if (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call)
                and isinstance(stmt.value.func, ast.Attribute)
                and stmt.value.func.attr == 'from_archive'
                and isinstance(stmt.value.func.value, ast.Name)
                and stmt.value.func.value.id == 'llvm'):
            files = next(kw.value for kw in stmt.value.keywords if kw.arg == 'files')
            return {dest: source_path(label) for dest, label in ast.literal_eval(files).items()}
    raise ValueError('Missing llvm.from_archive files in bazel/llvm.MODULE.bazel')
BUILD_OPTIONS_OUTPUT = 'src/Storages/System/StorageSystemBuildOptions.generated.cpp'
# CMake installs these headers unchanged beside the configured conf.h.
# Keep the upstream source tree authoritative instead of checking in copies.
COPIED_INCLUDE_DIRS = {
    'contrib/wasmtime/include': 'contrib/wasmtime/crates/c-api/include',
    'contrib/libcotp/include': 'contrib/libcotp/src',
}
CMAKE_ONLY_SOURCES = {'contrib/icu-cmake/empty.cpp'}
def cmake_build_options(text):
    """Preserve CMake feature/version data and extract compiler metadata."""
    substitutions = {
        '@BAZEL_COMPILER_VERSION_DEFINES@': '',
        '@BAZEL_BUILD_SYSTEM_ENTRY@': '',
    }
    for key in ['C_COMPILER', 'CXX_COMPILER', 'C_FLAGS', 'CXX_FLAGS', 'LINK_FLAGS',
                'CMAKE_VERSION', 'C_COMPILER_VERSION', 'CXX_COMPILER_VERSION']:
        pattern = r'(^    "' + key + r'", )(.*),$'
        matches = list(re.finditer(pattern, text, re.MULTILINE))
        if len(matches) != 1:
            raise ValueError(f'Expected one CMake build metadata value for {key}, found {len(matches)}')
        marker = '@BAZEL_' + key + '@'
        substitutions[marker] = matches[0][2]
        text = re.sub(pattern, lambda match: match[1] + marker + ',', text, flags=re.MULTILINE)
    text = text.replace('    "CMAKE_VERSION",', '@BAZEL_BUILD_SYSTEM_ENTRY@    "CMAKE_VERSION",')
    return '@BAZEL_COMPILER_VERSION_DEFINES@' + text, substitutions


class Graph:
    def __init__(self, arch):
        self.arch = arch
        self.build = ROOT / ('build_bazel_reference_' + arch)
        _, self.targets = read_file_api(self.build / '.cmake/api/v1/reply')
        self.config = ROOT / 'bazel/config' / arch
        self.rust_headers = set()
        self.header_cache = {}
        self.include_cache = {}
        self.missing = set()
        self.configured = set()
        self.source_headers = {}
        self.compiled = {n for n, t in self.targets.items() if t['type'] in COMPILE_TYPES}
        self.link_object_owners = {}
        for name, target in self.targets.items():
            owners = set()
            # INTERFACE libraries can forward TARGET_OBJECTS only through the
            # link command, without a File API dependency edge or source entry.
            for fragment in target.get('link', {}).get('commandFragments', []):
                for operand in shlex.split(fragment['fragment']):
                    if not operand.endswith('.o'):
                        continue
                    match = re.search(r'CMakeFiles/([^/]+)\.dir/', operand)
                    if not match or match[1] not in self.compiled:
                        raise ValueError('Unknown direct link object: ' + operand)
                    owners.add(match[1])
            self.link_object_owners[name] = owners
        self.direct_object_targets = set().union(*self.link_object_owners.values())
        self.id_names = {t['id']: n for n, t in self.targets.items()}
        self.codegen = Codegen(self)
        self.custom_outputs = {out for out, edge in self.codegen.nodes.items()
                               if edge['rule'] == 'CUSTOM_COMMAND'}
        self.build_options = {}
        self.parity_sources = {}
        self.embed_copies = set()

    def quoted_references(self, path, directive):
        if path not in self.include_cache:
            references = {'include': [], 'embed': []}
            for kind, value in re.findall(r'^\s*#\s*(include|embed)\s+"([^"]+)"', path.read_text(errors='replace'), re.MULTILINE):
                references[kind].append(value)
            self.include_cache[path] = references
        return self.include_cache[path][directive]

    def build_options_template(self, text):
        # Feature/version values come from the reference configuration. By
        # default, compiler metadata describes Bazel's selected toolchain;
        # comparison builds can replay the original CMake values exactly.
        for key in ['C_FLAGS', 'CXX_FLAGS']:
            value = re.search(r'^    "' + key + r'", (".*"),$', text, re.MULTILINE)
            self.build_options[key.lower()] = self.flags([{'fragment': json.loads(value[1])}])
        template, self.build_options['cmake_substitutions'] = cmake_build_options(text)
        return template

    def snapshot_configuration(self):
        self.configuration_rules = read_configurations(ROOT, self.build)
        for directory, dirs, names in os.walk(self.build):
            dirs[:] = [d for d in dirs if d not in {'CMakeFiles', '.cmake', 'native', 'cargo', 'external'}]
            for name in names:
                path = Path(directory) / name
                if path.suffix not in HEADER_SUFFIXES | {'.c', '.cc', '.cpp', '.S'} and name not in {'__config_site', '__assertion_handler'}:
                    continue
                rel = path.relative_to(self.build)
                dest = self.config / rel
                # Outputs remain owned by their generators after a CMake build.
                if rel.as_posix() in self.custom_outputs:
                    dest.unlink(missing_ok=True)
                    continue
                if (rel.is_relative_to(llvm_targets.PREFIX)
                        and not rel.is_relative_to(llvm_targets.PREFIX + '/llvm')):
                    continue
                for installed, source in COPIED_INCLUDE_DIRS.items():
                    if rel.is_relative_to(installed):
                        original = ROOT / source / rel.relative_to(installed)
                        if original.is_file():
                            if path.read_bytes() != original.read_bytes():
                                raise ValueError('Installed header differs from its source: ' + str(rel))
                            self.source_headers[rel.as_posix()] = original.relative_to(ROOT).as_posix()
                if rel.as_posix() in self.source_headers:
                    dest.unlink(missing_ok=True)
                    continue
                if rel.as_posix() == LICENSES_OUTPUT:
                    dest.unlink(missing_ok=True)
                    continue
                if rel.parts[0] == 'contrib':
                    if rel.as_posix() in CMAKE_ONLY_SOURCES:
                        continue
                    if rel.as_posix() not in self.configuration_rules and rel.as_posix() != 'contrib/cctz-cmake/TimeZones.generated.cpp':
                        raise ValueError('Untranslated configured input: ' + str(rel))
                    self.configured.add(rel.as_posix())
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                # Keep configuration relocatable; runtime build metadata is
                # regenerated alongside target data, never copied from a binary.
                text = path.read_text().replace(str(self.build), 'bazel/gen/' + self.arch).replace(str(ROOT) + '/', '')
                if rel.as_posix() == BUILD_OPTIONS_OUTPUT:
                    dest.unlink(missing_ok=True)
                    dest = dest.with_suffix('.cpp.in')
                    # Compatibility mode replays the exact reference metadata,
                    # including its path strings; do not relocate these values.
                    dest.write_text(self.build_options_template(path.read_text()))
                    continue
                dest.write_text(text)
                self.configured.add(rel.as_posix())
        # Empty CMake include directories do not need Bazel inputs.
        for marker in self.config.rglob('bazel_directory.h'):
            marker.unlink()

    def path(self, value):
        value = os.path.normpath(value)
        if value.startswith(str(self.build) + '/'):
            value = value.removeprefix(str(self.build) + '/')
            if value in self.source_headers:
                return self.source_headers[value]
            if value in self.configured:
                return 'bazel/config/' + self.arch + '/' + value
            self.missing.add(value)
            return 'bazel/gen/' + self.arch + '/' + value
        if value.startswith(str(ROOT) + '/'):
            return value.removeprefix(str(ROOT) + '/')
        if value.startswith('build_bazel_reference_' + self.arch + '/'):
            return self.path(str(ROOT / value))
        if os.path.isabs(value):
            raise ValueError('Undeclared host path: ' + value)
        return value

    def headers(self, directory, visited=None):
        directory = os.path.normpath(directory)
        for base in (ROOT, self.build):
            prefix = str(base / llvm_targets.PREFIX) + '/'
            if (directory.startswith(prefix)
                    and not directory.startswith((prefix + 'llvm/', prefix + 'third-party/siphash/'))):
                return []  # The registered toolchain owns runtime headers.
        if directory.startswith(str(self.build)):
            rel = os.path.relpath(directory, self.build)
            prefix = '' if rel == '.' else rel + '/'
            paths = [p for p in sorted(self.configured) if p.startswith(prefix)]
            if rel == 'contrib/librdkafka-cmake/auxdir':
                paths.append('contrib/librdkafka-cmake/config.h')
            return [source_label('bazel/config/' + self.arch + '/' + p) for p in paths] + [
                source_label(source) for p, source in sorted(self.source_headers.items()) if p.startswith(prefix)
            ]
        relative = self.path(directory)
        if relative.startswith('rust/workspace/'):
            return [source_label(p.relative_to(ROOT).as_posix())
                    for p in sorted((ROOT / relative).rglob('*'))
                    if p.is_file() and p.suffix in HEADER_SUFFIXES]
        if relative in {'.', ''}:
            raise ValueError('Repository-wide header glob requested')
        if relative.startswith('bazel/gen/'):
            return []
        if visited is None:
            visited = set()
        if relative in visited:
            return []
        visited.add(relative)
        result = [HeaderDirectory(relative)]
        # Headers can themselves include siblings outside the -I directory:
        # OpenLDAP's ldap-int.h includes ../liblber/lber-int.h, for example.
        # Follow only existing quoted includes that escape this declared root.
        if relative not in self.header_cache:
            root = ROOT / relative
            dependencies = set()
            for header in root.rglob('*'):
                if not header.is_file() or (header.suffix and header.suffix not in HEADER_SUFFIXES):
                    continue
                for include in self.quoted_references(header, 'include') + self.quoted_references(header, 'embed'):
                    path = Path(os.path.normpath(header.parent / include))
                    if not path.is_relative_to(root) and path.is_file():
                        dependencies.add(str(path.parent))
            self.header_cache[relative] = sorted(dependencies)
        for dependency in self.header_cache[relative]:
            result.extend(self.headers(dependency, visited))
        return sorted(set(result), key=input_key)

    def flags(self, fragments):
        result = []
        for fragment in fragments:
            for flag in shlex.split(fragment['fragment']):
                # The registered hermetic LLVM toolchain owns these paths.
                if flag.startswith(('--gcc-toolchain=', '--ld-path=', '-ffile-prefix-map=')):
                    continue
                if str(ROOT) in flag:
                    if flag.startswith(str(self.build) + '/'):
                        relative = flag.removeprefix(str(self.build) + '/')
                        if any(p.startswith(relative + '/') for p in self.configured):
                            flag = 'bazel/config/' + self.arch + '/' + relative
                        else:
                            flag = '$(GENDIR)/bazel/gen/' + self.arch + '/' + relative
                    flag = flag.replace(str(ROOT) + '/', '')
                result.append(flag)
                if flag == '-Weverything':
                    # cc_library emits both source and generated include dirs.
                    # RBE creates a generated dir only if it has action inputs.
                    result.append('-Wno-missing-include-dirs')
        return result

    def rust_cxx_configuration(self):
        cache = (self.build / 'CMakeCache.txt').read_text()
        result, headers = {}, set()
        for field, key in (('ccopts', 'RUST_CFLAGS'), ('copts', 'RUST_CXXFLAGS')):
            value = re.search(r'^' + key + r':INTERNAL=(.*)$', cache, re.MULTILINE)
            if not value:
                raise ValueError('Missing reference ' + key)
            flags = iter(self.flags([{'fragment': value[1]}]))
            copts = []
            for flag in flags:
                if flag == '--sysroot':
                    next(flags)  # The registered hermetic toolchain owns libc.
                    continue
                copts.append(flag)
                if flag in {'-I', '-isystem'}:
                    directory = next(flags)
                    if directory.startswith(llvm_targets.PREFIX + '/') or ('/' + llvm_targets.PREFIX + '/') in directory:
                        copts.pop()
                        continue
                    copts.append(directory)
                    headers.update(self.headers(str(ROOT / directory)))
            result[field] = copts
        self.rust_headers = headers
        labels = set()
        for header in headers:
            if isinstance(header, HeaderDirectory):
                labels.update(source_label(file) for file in directory_sources(header.directory))
            else:
                labels.add(header)
        return result | {'hdrs': sorted(labels)}

    def embedded_source(self, source, additional_inputs, flags):
        """Keep quoted #embed lookups inside Bazel's generated output tree.

        Clang resolves these paths relative to the source's real path, which
        escapes a symlink sandbox for checkout files. Copy the source and its
        source-relative assets together, preserving their bytes and layout.
        Root-relative assets also need this layout when fetched externally.
        """
        path = ROOT / source
        if not path.is_file():
            return source
        source_assets, root_assets = [], []
        for value in self.quoted_references(path, 'embed'):
            asset = Path(os.path.normpath(path.parent / value))
            if asset.is_file():
                source_assets.append(asset.relative_to(ROOT).as_posix())
            elif (ROOT / value).is_file():
                root_assets.append(value)
        assets = source_assets + root_assets
        if not assets:
            return source
        prefix = 'bazel/gen/' + self.arch + '/embedded/'
        self.embed_copies.update(assets)
        additional_inputs.extend(source_label(prefix + asset) for asset in assets)
        if root_assets:
            flags.append('--embed-dir=$(GENDIR)/' + prefix.rstrip('/'))
        if source_assets:
            self.embed_copies.add(source)
            return prefix + source
        return source

    def declare_configuration(self, builds, owners):
        for relative in sorted(self.configured):
            if not relative.startswith('contrib/'):
                continue
            owner = configuration_owner(relative)
            if owner not in builds:
                builds[owner] = BuildFile(owner, owners)
            build = builds[owner]
            output = 'config/' + self.arch + '/' + relative
            name = 'configure_' + self.arch + '_' + hashlib.sha256(relative.encode()).hexdigest()[:16]
            if relative == 'contrib/cctz-cmake/TimeZones.generated.cpp':
                build.rule('genrule', name, {
                    'srcs': Glob(['testdata/zoneinfo/**'], (
                        'testdata/zoneinfo/**/*.tab', 'testdata/zoneinfo/**/localtime',
                    )),
                    'outs': [output],
                    'tools': ['@clickhouse//bazel/rules:generate_timezones'],
                    'cmd': '$(execpath @clickhouse//bazel/rules:generate_timezones) --output=$@ $(SRCS)',
                })
                continue
            spec = self.configuration_rules[relative]
            if spec['kind'] == 'write':
                build.rule('write_file', name, {'out': output, 'content': [spec['content']]})
                continue
            source = build.label(source_label(spec['source'].resolve().relative_to(ROOT).as_posix()))
            if spec['kind'] == 'copy':
                build.rule('copy_file', name, {'src': source, 'out': output})
            else:
                substitutions = {
                    key: value.replace(str(self.build), 'bazel/gen/' + self.arch).replace(str(ROOT) + '/', '')
                    for key, value in spec['substitutions'].items()
                }
                for key in ('@CMAKE_CXX_COMPILER_VERSION@', '@MONGOC_CXX_COMPILER_VERSION@'):
                    if key in substitutions:
                        substitutions[key] = CMakeCxxCompilerVersion(self.arch)
                mongo_cxx_version = '#cmakedefine MONGOCXX_COMPILER_VERSION "${MONGOCXX_COMPILER_VERSION}"'
                if mongo_cxx_version in substitutions:
                    substitutions[mongo_cxx_version] = CMakeCxxCompilerVersion(
                        self.arch, '#define MONGOCXX_COMPILER_VERSION "', '"')
                build.rule('expand_template', name, {'template': source, 'out': output, 'substitutions': substitutions})
        # These were generated snapshots, never authoritative source inputs.
        if (self.config / 'contrib').exists():
            shutil.rmtree(self.config / 'contrib')

    def target(self, name, target, native=False):
        if llvm_targets.native_target(name, target):
            return {'alias': '@' + llvm_targets.RAW_REPOSITORY + '//:' + name,
                    'kind': target['type'], 'groups': [], 'deps': [], 'linkopts': [], 'alwayslink': False}
        module_target = llvm_targets.target(name)
        if module_target:
            result = {'groups': [], 'deps': [], 'linkopts': [], 'kind': target['type'], 'alwayslink': False}
            if target['type'] == 'EXECUTABLE':
                return result | {'alias': module_target}
            dependencies = [self.id_names[d['id']] for d in target.get('dependencies', [])]
            result['deps'] = [':' + d for d in sorted(dependencies)
                              if d in self.compiled and self.targets[d]['type'] != 'EXECUTABLE']
            result['deps'].insert(0, module_target)
            if name == '_fuzzer':
                result['deps'].append('@llvm-project//compiler-rt:fuzzer')
            return result
        if name in BCR_DEPENDENCIES and not native:
            return {'groups': [], 'deps': BCR_DEPENDENCIES[name]['deps'],
                    'linkopts': [], 'kind': target['type'], 'alwayslink': False}
        groups = []
        for index, cg in enumerate(target.get('compileGroups', [])):
            sources = []
            for i in cg['sourceIndexes']:
                path = target['sources'][i]['path']
                absolute = Path(os.path.normpath(ROOT / path))
                if absolute.is_relative_to(self.build) and absolute.relative_to(self.build).as_posix() in CMAKE_ONLY_SOURCES:
                    if absolute.read_text().strip():
                        raise ValueError('CMake-only translation unit is no longer empty: ' + path)
                    continue
                sources.append(self.path(path))
            if not sources:
                continue
            flags = self.flags(cg.get('compileCommandFragments', []))
            generated_inputs = self.codegen.target_generated_inputs(name)
            hdrs = {p for p in generated_inputs if Path(p).suffix in HEADER_SUFFIXES}
            for i, flag in enumerate(flags[:-1]):
                # Bare names such as XGBoost's -include exception resolve
                # through the declared include roots or compiler toolchain.
                # Only direct workspace files need an additional input here.
                if flag in {'-include', '-imacros'} and (ROOT / flags[i + 1]).is_file():
                    hdrs.add(source_label(flags[i + 1]))
            # Arrow includes generated Protobuf implementation files as text.
            textual_hdrs = [p for p in generated_inputs if p.endswith('.cc')]
            additional_inputs = [p for p in generated_inputs if p.endswith('.gz')]
            for flag in flags:
                if flag.startswith('--embed-dir='):
                    additional_inputs.extend(self.headers(str(ROOT / flag.split('=', 1)[1])))
            generated_paths = {p.removeprefix('//bazel:gen/' + self.arch + '/')
                               for p in generated_inputs if p.startswith('//bazel:gen/')}
            generated_paths.update(s.removeprefix('bazel/gen/' + self.arch + '/')
                                   for s in sources if s.startswith('bazel/gen/'))
            external_deps = set()
            arrow_deps = None
            native_cxx_includes = []
            for src in sources:
                if not src.startswith('bazel/'):
                    hdrs.update(self.headers(str(ROOT / src).rsplit('/', 1)[0]))
                    # Quoted relative includes can reach outside both the source
                    # directory and CMake's -I paths (e.g. zxc's ../../include).
                    source = ROOT / src
                    for include in self.quoted_references(source, 'include'):
                        header = Path(os.path.normpath(source.parent / include))
                        if header.is_file():
                            hdrs.update(self.headers(str(header.parent)))
                            if native and header.suffix in {'.c', '.cc', '.cpp'}:
                                additional_inputs.append(source_label(header.relative_to(ROOT).as_posix()))
            for inc in cg.get('includes', []):
                flag = '-isystem' if inc.get('isSystem') else '-I'
                path = os.path.normpath(inc['path'])
                relative = path.removeprefix(str(self.build) + '/').removeprefix(str(ROOT) + '/')
                if relative in llvm_targets.TOOLCHAIN_INCLUDE_DIRECTORIES:
                    if cg['language'] in {'C', 'ASM', 'ASM_NASM'}:
                        native_cxx_includes.append([
                            flag, 'generated' if path.startswith(str(self.build) + '/') else 'source',
                            relative.removeprefix(llvm_targets.PREFIX + '/'),
                        ])
                    continue
                if (path in {str(ROOT / 'contrib/arrow/cpp/src'), str(self.build / 'contrib/arrow-cmake/cpp/src')}
                        and target_owner(target) != 'contrib/arrow'):
                    if arrow_deps is None:
                        arrow_deps = foreign_arrow_dependencies(
                            ROOT, name, index, [ROOT / target['sources'][i]['path'] for i in cg['sourceIndexes']], self.arch)
                    continue
                module_dependencies = llvm_targets.include_dependencies(relative)
                if module_dependencies is not None:
                    external_deps.update(module_dependencies)
                    continue
                if path == str(self.build / 'contrib/librdkafka-cmake/auxdir'):
                    path = str(self.build / 'contrib/librdkafka-cmake')
                if path == str(self.build / 'contrib/delta-kernel-rs/include'):
                    external_deps.add('//bazel/rust:delta_kernel_ffi_cc')
                    continue
                replacement = next((d for dep_name, d in BCR_DEPENDENCIES.items()
                                    if not (native and dep_name == name)
                                    and path.removeprefix(str(ROOT) + '/') in d['include_directories']), None)
                if replacement:
                    external_deps.update(replacement['deps'])
                    continue
                # First-party generated headers come from the C++ dependency that owns them.
                if path == str(self.build) and (
                    target.get('paths', {}).get('source', '').split('/')[0] in {'base', 'programs', 'src'}
                    or all(s.startswith('programs/') for s in sources)
                ):
                    continue
                hdrs.update(self.headers(path))
                if path.startswith(str(self.build)):
                    rel = os.path.relpath(path, self.build)
                    prefix = '' if rel == '.' else rel + '/'
                    directories = []
                    configured_directory = 'bazel/config/' + self.arch + '/' + rel
                    if any(p.startswith(prefix) for p in self.configured):
                        directories.append(configured_directory)
                    if rel in COPIED_INCLUDE_DIRS:
                        directories.append(COPIED_INCLUDE_DIRS[rel])
                    # Package codegen writes into the same directory as configure_file.
                    if (any(p.startswith(prefix) for p in generated_paths)
                            and not (prefix in PACKAGE_CODEGEN_DIRECTORIES and configured_directory in directories)):
                        directories.append('$(GENDIR)/bazel/gen/' + self.arch + '/' + rel)
                    # Only declared inputs populate Bazel's generated tree;
                    # empty CMake include roots have no files to find here.
                    for directory in directories:
                        flags += [flag, directory]
                else:
                    flags += [flag, self.path(path)]
            selected = set(embedded_indices(name, sources))
            embedded_inputs = {}
            for i, source in enumerate(sources):
                inputs = [] if i in selected else additional_inputs
                sources[i] = self.embedded_source(source, inputs, flags)
                if i in selected:
                    if sources[i] == source or not inputs or cg['language'] != 'CXX':
                        raise ValueError(f'{name}: missing C++ #embed copy or assets for {source}')
                    embedded_inputs[source_label(sources[i])] = inputs
                    additional_inputs.extend(inputs)
            public_arrow, private_arrow = arrow_deps or ((), ())
            if target['type'] == 'EXECUTABLE' and index == 0:
                public_arrow = [*public_arrow, *private_arrow]
                private_arrow = []
            groups.append({
                'srcs': [source_label(s) for s in sources],
                'embedded_inputs': embedded_inputs,
                'copts': flags,
                'cmake_source_prefix_map': cmake_has_source_prefix_map(cg.get('compileCommandFragments', []), ROOT),
                # cc_library tokenizes defines with Bourne-shell rules. Preserve
                # literal quotes (e.g. Cyrus SASL's CONFIGDIR="") through that step.
                'local_defines': [shlex.quote(d['define'].replace(str(ROOT) + '/', '')) for d in cg.get('defines', [])],
                'hdrs': sorted(hdrs, key=input_key),
                'textual_hdrs': textual_hdrs,
                'additional_compiler_inputs': sorted(set(additional_inputs), key=input_key),
                'native_cxx_includes': native_cxx_includes,
                'deps': sorted(external_deps),
                **({'header_deps': public_arrow} if public_arrow else {}),
                **({'implementation_deps': private_arrow} if private_arrow else {}),
                'language': cg['language'],
            })
        if name in EMBEDDED_SOURCES and sum(bool(g['embedded_inputs']) for g in groups) != 1:
            raise ValueError(f'{name}: expected one CMake C++ group with #embed sources')
        dependencies = {self.id_names[d['id']] for d in target.get('dependencies', [])}
        object_refs = set()
        generated_objects = []
        for src in target.get('sources', []):
            path = self.path(src['path'])
            output = path.removeprefix('bazel/gen/' + self.arch + '/')
            if output.endswith('.o') and output in self.codegen.handled:
                generated_objects.append(source_label(path))
            match = re.search(r'CMakeFiles/([^/]+)\.dir/', src['path'])
            if match and match[1] in self.compiled:
                object_refs.add(match[1])
        if generated_objects:
            groups.append({'srcs': generated_objects, 'language': 'OBJECT'})
        deps = [':' + d for d in sorted((dependencies | object_refs | self.link_object_owners[name]) & self.compiled)
                if self.targets[d]['type'] != 'EXECUTABLE']
        if name == '_zxc':
            # zxc-cmake passes $<TARGET_OBJECTS:...> to add_library(_zxc).
            group, = groups
            variants = [d for d in deps if d[1:] in object_refs and self.targets[d[1:]]['type'] == 'OBJECT_LIBRARY']
            group.setdefault('implementation_deps', []).extend(variants)
            deps = [d for d in deps if d not in variants]
        linkopts = []
        if target['type'] == 'EXECUTABLE':
            # Link-only flags are retained separately from the archive closure.
            for flag in self.flags([f for f in target.get('link', {}).get('commandFragments', []) if f['role'] == 'flags']):
                if flag.endswith('.a') or flag in {'-Wl,--whole-archive', '-Wl,--no-whole-archive', '-llibllvmlibc'}:
                    continue
                linkopts.append(flag)
            for frag in target.get('link', {}).get('commandFragments', []):
                if frag['role'] == 'libraries' and frag['fragment'].startswith('-l'):
                    linkopts.append(frag['fragment'])
                if frag['role'] == 'libraries':
                    for library in shlex.split(frag['fragment']):
                        library = library.removeprefix(str(self.build) + '/')
                        if library in RUST_ARCHIVES:
                            deps.append('//bazel/rust:' + RUST_ARCHIVES[library] + '_cc')
            # CMake loads these whole archives before all ordinary archives.
            # In particular, llvm-libc's math definitions must be seen before
            # the lazy glibc compatibility archive, which also supplies them.
            runtimes = [':' + n for n in ['clang_rt_builtins', 'clang_rt_xray', 'libllvmlibc']
                        if n in self.compiled]
            deps = runtimes + [d for d in deps if d not in runtimes]
        # TARGET_OBJECTS can also forward a STATIC_LIBRARY's objects directly.
        alwayslink = (target['type'] in {'OBJECT_LIBRARY', 'EXECUTABLE'}
                      or name in self.direct_object_targets
                      or name in {'clang_rt_builtins', 'clang_rt_xray', 'libllvmlibc'})
        return {'groups': groups, 'deps': deps, 'linkopts': linkopts,
                'kind': target['type'], 'alwayslink': alwayslink}

    def generate(self):
        self.snapshot_configuration()
        self.codegen.generate()
        self.codegen.licenses(LICENSES_OUTPUT)
        data = {name: self.target(name, t) for name, t in sorted(self.targets.items()) if t['type'] in COMPILE_TYPES}
        self.codegen.embedded_sources()
        for target in data.values():
            for group in target['groups']:
                for source in group['srcs']:
                    configured = re.match(r'@([^/]+)//:config/' + self.arch + r'/(contrib/.*)', source)
                    if configured:
                        relative = configured[2]
                        owner = configuration_owner(relative)
                        identity = '@build/' + self.arch + '/' + relative
                        self.parity_sources['build_bazel_reference_' + self.arch + '/' + relative] = identity
                        self.parity_sources['bazel-out/k8-opt/bin/' + comparison_root(owner) + '/config/' + self.arch + '/' + relative] = identity
                        continue
                    original = source_path(source)
                    repo = repository(original)
                    if repo:
                        self.parity_sources[comparison_root(repo) + original.removeprefix(repo)] = original
                    for kind in ['config', 'gen']:
                        prefix = '//bazel:' + kind + '/' + self.arch + '/'
                        if not source.startswith(prefix):
                            continue
                        relative = source.removeprefix(prefix)
                        if kind == 'gen' and relative.startswith('embedded/'):
                            self.parity_sources['bazel-out/k8-opt/bin/bazel/gen/' + self.arch + '/' + relative] = relative.removeprefix('embedded/')
                            continue
                        identity = '@build/' + self.arch + '/' + relative
                        self.parity_sources['build_bazel_reference_' + self.arch + '/' + relative] = identity
                        bazel_path = source.replace('//bazel:', 'bazel/', 1)
                        if kind == 'gen':
                            bazel_path = 'bazel-out/k8-opt/bin/' + bazel_path
                        self.parity_sources[bazel_path] = identity
        # Module-owned libraries no longer pass through translated compile
        # groups, but their actual source files remain in the parity audit.
        module_root = llvm_targets.TOOLCHAIN_ROOT
        for name in self.compiled:
            if not llvm_targets.target(name):
                continue
            target = self.targets[name]
            for group in target.get('compileGroups', []):
                for index in group['sourceIndexes']:
                    source = target['sources'][index]['path']
                    if source.startswith(llvm_targets.PREFIX + '/'):
                        self.parity_sources[module_root + source.removeprefix(llvm_targets.PREFIX)] = source
        for destination, source in llvm_source_overlays().items():
            self.parity_sources[module_root + '/' + destination] = source
        self.build_options.update({
            'template': 'config/' + self.arch + '/' + BUILD_OPTIONS_OUTPUT + '.in',
            'out': 'gen/' + self.arch + '/' + BUILD_OPTIONS_OUTPUT,
            'link_flags': data['clickhouse']['linkopts'],
        })
        self.codegen.handled.add(BUILD_OPTIONS_OUTPUT)
        self.generated_targets = data
        translated_outputs = self.codegen.handled | {
            output.removeprefix('gen/' + self.arch + '/')
            for rule in self.codegen.rules.values() for output in rule['outs']
        }
        substitutions = {
            output: '//bazel/rust:' + name + '_processed'
            for output, name in RUST_ARCHIVES.items() if output in self.custom_outputs
        }
        # compile_et is a shell wrapper around et_h.awk/et_c.awk. Their native
        # generator actions invoke those exact scripts directly.
        substitutions['contrib/krb5-cmake/compile_et'] = 'direct et_h.awk/et_c.awk actions'
        substitutions.update({output: llvm_targets.generated_target(output) for output in self.custom_outputs
                              if llvm_targets.generated_target(output)})
        remaining = self.custom_outputs - translated_outputs - substitutions.keys()
        classified, untranslated = self.classify_custom_outputs(remaining)
        return {
            'targets': len(self.targets),
            'native_targets': len(data),
            'compile_actions': sum(len(g['srcs']) for t in data.values() for g in t['groups'] if g['language'] != 'OBJECT'),
            'generated_object_inputs': sum(len(g['srcs']) for t in data.values() for g in t['groups'] if g['language'] == 'OBJECT'),
            'bcr_targets': sorted(BCR_DEPENDENCIES),
            'codegen_actions': len(self.codegen.rules) + 1,  # Toolchain build metadata.
            'native_substitutions': dict(sorted(substitutions.items())),
            'cmake_only_outputs': classified,
            'untranslated_outputs': untranslated,
            'configure_outputs': sorted(self.configured),
            'source_headers': dict(sorted(self.source_headers.items())),
            'build_outputs': sorted(self.missing),
            'utility_targets': sorted(n for n, t in self.targets.items() if t['type'] == 'UTILITY'),
        }

    def classify_custom_outputs(self, outputs):
        """Keep replaced bookkeeping and maintenance commands visible by purpose."""
        categories = {
            'cmake_cache': 'CMake cache editing and regeneration; not build artifacts.',
            'cmake_install': 'CMake installation commands; Bazel packaging is not implemented.',
            'ctest': 'CTest entry points; unit_tests_dbms is a native cc_test.',
            'cargo_build': 'Replaced by the seven native rules_rs archive targets.',
            'rust_symbols': 'Replaced by the seven *_processed archive targets.',
            'cargo_clean': 'Cargo maintenance commands; Bazel manages its own outputs.',
            'googleapis_external_project': 'ExternalProject stamps; native actions extract the pinned schema archive.',
            'tablegen_external_project': 'ExternalProject stamps; native execution-platform TableGen targets replace the nested build.',
            'contributors': 'Manual release maintenance; the build consumes the checked-in contributors source.',
        }
        classified = {key: {'reason': reason, 'outputs': []} for key, reason in categories.items()}
        rust_names = {Path(path).stem.removeprefix('lib') for path in RUST_ARCHIVES}
        external_stages = {'build', 'configure', 'done', 'download', 'install', 'mkdir', 'patch', 'update'}
        unknown = []
        for output in sorted(outputs):
            command = self.codegen.nodes[output].get('command', '')
            name = output.split('CMakeFiles/', 1)[-1]
            category = None
            if name == 'edit_cache.util' and command.endswith('/usr/bin/cmake -E echo No\\ interactive\\ CMake\\ dialog\\ available.'):
                category = 'cmake_cache'
            elif name == 'rebuild_cache.util' and '/usr/bin/cmake --regenerate-during-build ' in command:
                category = 'cmake_cache'
            elif name in {'install.util', 'install/local.util', 'install/strip.util'} and command.endswith(' -P cmake_install.cmake'):
                category = 'cmake_install'
            elif name == 'test.util' and command.endswith('/usr/bin/ctest --force-new-ctest-process'):
                category = 'ctest'
            elif name.removesuffix('.util').removeprefix('_cargo-build_') in rust_names and name.startswith('_cargo-build_'):
                category = 'cargo_build'
            elif name.removeprefix('localize_rust_c_') in rust_names and name.startswith('localize_rust_c_'):
                category = 'rust_symbols'
            elif name in {'strip_pco_symbols', 'strip_polyglot_symbols', 'strip_prql_symbols'}:
                category = 'rust_symbols'
            elif name.removeprefix('cargo-clean_') in rust_names and name.startswith('cargo-clean_'):
                category = 'cargo_clean'
            elif output == 'contrib/google-cloud-cpp-cmake/CMakeFiles/googleapis_download-complete' or output.removeprefix('external/googleapis/src/googleapis_download-stamp/googleapis_download-') in external_stages:
                category = 'googleapis_external_project'
            elif output == 'contrib/llvm-project-cmake/CMakeFiles/native-tblgen-build-complete' or output.removeprefix('contrib/llvm-project-cmake/native-tblgen-build-prefix/src/native-tblgen-build-stamp/native-tblgen-build-') in external_stages:
                category = 'tablegen_external_project'
            elif output == 'src/Storages/System/CMakeFiles/generate-contributors' and command.endswith(' && ./StorageSystemContributors.sh'):
                category = 'contributors'
            if category:
                classified[category]['outputs'].append(output)
            else:
                unknown.append(output)
        return {key: value for key, value in classified.items() if value['outputs']}, unknown


def main():
    # Keep the reviewed public/private headers, msgpack_c_api, the Google Cloud
    # protobuf and private dependencies, and the ZXC per-instruction-set libraries.
    abseil = ROOT / 'bazel/dependencies/clickhouse_abseil_cpp.BUILD.bazel'
    arrow = ROOT / 'bazel/dependencies/clickhouse_arrow.BUILD.bazel'
    msgpack = ROOT / 'bazel/dependencies/clickhouse_msgpack_c.BUILD.bazel'
    google_cloud = ROOT / 'bazel/dependencies/clickhouse_google_cloud_cpp.BUILD.bazel'
    zxc = ROOT / 'bazel/dependencies/clickhouse_zxc.BUILD.bazel'
    # gRPC declares the public and private headers for grpc and grpc_unsecure.
    # PostgreSQL and Vectorscan own the headers installed by _CONFIGURED_FILES.
    read_only = {abseil, arrow, msgpack, google_cloud, zxc,
                 ROOT / 'bazel/dependencies/clickhouse_capnproto.BUILD.bazel',
                 ROOT / 'bazel/dependencies/clickhouse_grpc.BUILD.bazel',
                 ROOT / 'bazel/dependencies/clickhouse_postgres.BUILD.bazel',
                 ROOT / 'bazel/dependencies/clickhouse_rocksdb.BUILD.bazel',
                 ROOT / 'bazel/dependencies/clickhouse_vectorscan.BUILD.bazel'}
    maintained = {path: path.read_bytes() for path in read_only}
    from initial_migration.compact_build import Source
    arrow_targets = {kw.value.value for _, rule in Source(maintained[arrow].decode()).rules()
                    for kw in rule.keywords if kw.arg == 'name' and isinstance(kw.value, ast.Constant)}
    licenses = subprocess.check_output(
        ['bash', str(ROOT / 'utils/list-licenses/list-licenses.sh'), str(ROOT)],
        cwd=ROOT, text=True,
    )
    license_manifest = [line.split('\t') for line in licenses.splitlines()]
    if any(len(row) != 3 for row in license_manifest):
        raise ValueError('Unexpected canonical license inventory format')
    (ROOT / 'bazel/rules/license_manifest.json').write_text(json.dumps(license_manifest, indent=2) + '\n')
    graphs = {arch: Graph(arch) for arch in ARCHES}
    owners = {}
    for graph in graphs.values():
        for name in sorted(graph.compiled):
            owner = None if (name in BCR_DEPENDENCIES or llvm_targets.target(name)
                             or llvm_targets.native_target(name, graph.targets[name])) else target_owner(graph.targets[name])
            if name in owners and owners[name] != owner:
                raise ValueError("Repository owner differs across architectures: " + name)
            owners[name] = owner
    inventories = {}
    codegen_rules = {}
    build_options = {}
    cmake_metadata = read_metadata(ROOT)
    rust_cxx = {}
    parity_sources = {}
    for arch in ARCHES:
        graph = graphs[arch]
        inventories[arch] = graph.generate()
        rust_cxx[arch] = graph.rust_cxx_configuration()
        codegen_rules.update(graph.codegen.rules)
        build_options[arch] = graph.build_options
        cmake_metadata[arch]['build_options'] = build_options[arch].pop('cmake_substitutions')
        parity_sources.update(graph.parity_sources)
    google_schemas = google_api_schemas(ROOT)
    codegen_rules = common_googleapis_rules(relocate_tools(codegen_rules, owners), google_schemas)
    # Keep CMake node names until Graph.generate() finishes indexing generators.
    # Only the Google APIs input/compile directories are configuration-neutral.
    for graph in graphs.values():
        for target in graph.generated_targets.values():
            for group in target['groups']:
                for field in ('srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs', 'copts'):
                    if field in group:
                        group[field] = [googleapis_path(value) for value in group[field]]
    parity_sources = googleapis_source_map(parity_sources)
    # Count CMake target names across both platforms, not generated compile
    # group names. A directory consumed only by one target is private.
    native_lz4 = {arch: graph.target('_lz4', graph.targets['_lz4'], native=True)
                  for arch, graph in graphs.items() if '_lz4' in graph.targets}
    consumers = collections.defaultdict(set)
    for arch, graph in graphs.items():
        targets = dict(graph.generated_targets, _lz4=native_lz4.get(arch, {'groups': []}))
        for name, target in targets.items():
            for group in target['groups']:
                for field in ('hdrs', 'additional_compiler_inputs'):
                    for item in group.get(field, []):
                        if isinstance(item, HeaderDirectory):
                            consumers[item.directory].add(name)
        for item in graph.rust_headers:
            if isinstance(item, HeaderDirectory):
                consumers[item.directory].add('rust_cxx')
    targets = {arch: graph.generated_targets for arch, graph in graphs.items()}
    external = HeaderLibraries(targets)
    native_sources = {
        src for arch_targets in targets.values() for target in arch_targets.values()
        for group in target.get('groups', ())
        if group.get('language') in {'C', 'CXX', 'ASM'} and not group.get('cmake_runtime')
        for src in group.get('srcs', ())
    }
    foreign = relocate_compilation(owners, targets)
    builds = {owner: BuildFile(owner, owners, consumers, targets, external_headers=external,
                               native_sources=native_sources)
              for owner in {None, *owners.values()}}
    for graph in graphs.values():
        graph.declare_configuration(builds, owners)
    # Preserve the CMake declarations for checking both CPUs and rewriting consumer labels.
    configuration_inputs = {overlay: '\n'.join(builds[owner].rules)
                            for overlay, owner in ((arrow, 'contrib/arrow'), (google_cloud, 'contrib/google-cloud-cpp'))}
    for name, owner in sorted(owners.items()):
        if owner == 'contrib/arrow':
            if name not in arrow_targets:
                raise ValueError(f'{arrow} does not declare CMake target {name}')
            continue
        builds[owner].target(name, {arch: graph.generated_targets[name]
                                  for arch, graph in graphs.items() if name in graph.generated_targets})
    for (owner, name, input_name), variants in sorted(foreign.items()):
        if owner == 'contrib/arrow':
            if name not in arrow_targets:
                raise ValueError(f'{arrow} does not declare relocated CMake target {name}')
            continue
        build = builds.setdefault(owner, BuildFile(
            owner, owners, consumers, targets, external_headers=external, native_sources=native_sources))
        build.target(name, variants, input_name=input_name)
    for owner, directories in SOURCE_HEADER_DIRECTORIES.items():
        build = builds.setdefault(owner, BuildFile(owner, owners, consumers, targets))
        patterns = [directory + '/**/*' + suffix for directory in directories
                    for suffix in SOURCE_HEADER_SUFFIXES.get(owner, CC_HEADER_SUFFIXES)]
        attrs = {'hdrs': Glob(patterns)}
        if owner == 'contrib/libpqxx':
            # The pinned include/pqxx extensionless files are the public
            # wrapper headers, e.g. #include <pqxx/connection>.
            attrs['textual_hdrs'] = Glob(['include/pqxx/*'], ['include/pqxx/*.*'])
        build.rule('cc_library', SOURCE_HEADER_TARGETS[owner], attrs)
    external.declare(
        builds, lambda owner: BuildFile(owner, owners, consumers, targets, external_headers=external))
    # The public _lz4 wrapper serves rules_rs/BCR clients; the CMake link uses
    # the original group's ordered objects from its own pinned repository.
    lz4 = builds.setdefault('contrib/lz4', BuildFile('contrib/lz4', owners | {'_lz4': 'contrib/lz4'}, consumers))
    for target in native_lz4.values():
        group, = target['groups']
        group['hdrs'].remove(HeaderDirectory('contrib/lz4/lib'))
        # The native archive uses only the headers paired with its four C sources.
        group['srcs'] += [src.removesuffix('.c') + '.h' for src in group['srcs']]
    lz4.target('_lz4__compile_0', native_lz4, input_name='_lz4')
    declare_boost_header_libraries(builds['contrib/boost'])
    llvm, llvm_native_targets, llvm_root_inputs = llvm_targets.declare_from_references(
        ROOT, {arch: graph.build for arch, graph in graphs.items()})
    builds[llvm_targets.PREFIX] = llvm
    dependency_inputs(builds, owners)
    root = builds[None]
    # Keep every configured dependency in //... even when it is only an
    # auxiliary library or tool outside the server's transitive closure.
    root.rule('filegroup', 'contrib', {'srcs': select({
        '@platforms//cpu:' + arch: [target_label(name, owners) for name in sorted(graph.compiled) if owners[name]]
        for arch, graph in graphs.items()
    })})
    llvm_overlay_inputs = set(llvm_source_overlays().values())
    rust_root_inputs = {label[3:] for config in rust_cxx.values()
                        for label in config['hdrs'] if label.startswith('//:')}
    root_exports = sorted(set().union(*(build.root_sources for build in builds.values()))
                          | llvm_overlay_inputs | llvm_root_inputs | rust_root_inputs)
    (ROOT / 'BUILD.bazel').write_text(
        root.load_statements() +
        'load("//bazel/initial_migration:root_inputs.bzl", "declare_inputs")\n\n'
        'package(default_visibility = ["//visibility:public"])\n\n'
        'declare_inputs()\n\nexports_files(' + starlark(root_exports) + ')\n\n' + root.text())
    (ROOT / 'bazel/headers.bzl').unlink(missing_ok=True)
    (ROOT / 'bazel/initial_migration/coverage.json').write_text(json.dumps(inventories, indent=2) + '\n')
    preamble = 'load("//bazel/rules:repository_paths.bzl", "repository_path")\n'
    (ROOT / 'bazel/rules/codegen_rules.bzl').write_text(render_rules(codegen_rules, google_schemas))
    (ROOT / 'bazel/rules/build_options_data.bzl').write_text(preamble + 'BUILD_OPTIONS = ' + starlark(build_options) + '\n')
    write_metadata(ROOT, cmake_metadata)
    (ROOT / 'bazel/rules/rust_cxx_flags.bzl').write_text(preamble + 'RUST_CXX = ' + starlark(rust_cxx) + '\n')
    # An external cc_library otherwise exports a quote include root that takes
    # precedence over CMake's system include path. Preserve the system boundary
    # with rules_cc's standard feature where every reference use agrees.
    repository_includes = collections.defaultdict(set)
    for graph in graphs.values():
        for target in graph.targets.values():
            for group in target.get('compileGroups', []):
                for include in group.get('includes', []):
                    path = include['path'].removeprefix(str(ROOT) + '/')
                    if path in REPOSITORIES:
                        repository_includes[path].add(bool(include.get('isSystem')))
    system_repositories = {path for path, kinds in repository_includes.items() if kinds == {True}}
    write_overlays(builds, [codegen_rules, rust_cxx], system_repositories, read_only=read_only)
    repository_files = {owner: package_sources(owner) for owner in builds if owner in REPOSITORIES}
    prune(ROOT, repository_files, write=True, read_only=read_only)
    prune_unused_exports(ROOT, write=True, read_only=read_only)
    parity_path = ROOT / 'bazel/initial_migration/any2bazel.json'
    parity = json.loads(parity_path.read_text())
    parity['target_map'] = {
        name: (llvm_targets.target(name).replace('@llvm-project', '@@' + llvm_targets.TOOLCHAIN_ROOT.removeprefix('external/'))
               if llvm_targets.target(name) else
               ('@@' + comparison_root(owner).removeprefix('external/') + '//:' if owner else ':') + name)
        for name, owner in sorted(owners.items())
        if any(graph.targets.get(name, {}).get('type') == 'EXECUTABLE' for graph in graphs.values())
    }
    # any2bazel source_map replaces the ENTIRE source path, unlike its include
    # map. Map individual files so distinct generated TUs cannot collapse.
    parity['ignore']['source_map'] = [{'from': source, 'to': identity}
                                      for source, identity in sorted(parity_sources.items())]
    generated_includes = [
        {'from': comparison_root(prefix), 'to': prefix} for prefix in sorted(REPOSITORIES)
    ] + [
        {'from': 'bazel-out/k8-opt/bin/' + comparison_root(prefix) + '/config/' + arch, 'to': '@build/' + arch}
        for arch, graph in graphs.items()
        for prefix in sorted({configuration_owner(path) for path in graph.configured if path.startswith('contrib/')})
    ]
    generated_includes += llvm_targets.include_mappings(llvm_targets.TOOLCHAIN_ROOT)
    for arch in ARCHES:
        generated_includes += [
            {'from': 'build_bazel_reference_' + arch + '/contrib/libcotp/include',
             'to': COPIED_INCLUDE_DIRS['contrib/libcotp/include']},
            {'from': 'build_bazel_reference_' + arch + '/contrib/librdkafka-cmake/auxdir',
             'to': '@build/' + arch + '/contrib/librdkafka-cmake'},
        ]
    generated_prefixes = {row['from'] for row in generated_includes}
    existing = [row for row in parity['ignore']['include_map']
                if row['from'] not in generated_prefixes and not row['from'].startswith((
                    'external/+contrib+', 'bazel-out/k8-opt/bin/external/+contrib+',
                    'external/llvm++llvm+llvm-project',
                    'bazel-out/k8-opt/bin/external/llvm++llvm+llvm-project'))]
    parity['ignore']['include_map'] = existing + generated_includes
    llvm_targets.update_any2bazel_mappings(parity, llvm_native_targets)
    parity_path.write_text(json.dumps(parity, indent=2) + '\n')
    (ROOT / 'REPO.bazel').write_text(
        'ignore_directories(' + starlark(['build', 'build_*', 'tmp'] + sorted(REPOSITORIES)) + ')\n')
    from initial_migration.configured_external_outputs import Migration as ExternalConfigs
    from initial_migration.configured_codegen_outputs import Migration as ExternalCodegen
    from initial_migration.configured_llvm_outputs import Migration as LlvmOutputs
    ExternalConfigs(ROOT, source_overrides=configuration_inputs).run(write=True)
    ExternalCodegen(ROOT).run(write=True, read_only=read_only)
    from initial_migration.package_codegen import publish_package_codegen
    publish_package_codegen(ROOT, read_only=read_only)
    LlvmOutputs(ROOT).run(write=True, read_only=read_only)
    # Capture CMake at repository scope, then render native Bazel packages.
    # Existing native BUILD files must not hide CMake inputs on a second run.
    from initial_migration.first_party_packages import Build, Migration as NativePackages, native_directory
    native_packages = NativePackages(ROOT, stage='all', from_generator=True)
    native_builds = native_packages.prepare()
    for path, content in native_builds.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    from initial_migration.compact_external_options import (
        PackageFiles, compact as compact_external_options, format_and_prove, source_globs,
    )
    from initial_migration.normalize_cpp_builds import drop_defaults, normalize, prove
    for owner, spec in REPOSITORIES.items():
        path = ROOT / 'bazel/dependencies' / (spec['name'] + '.BUILD.bazel')
        if path.is_file() and path not in read_only:
            content, _ = compact_external_options(path.read_text(), spec['name'], package_sources(owner))
            path.write_text(normalize(content))
    from initial_migration.configured_native_snapshots import Migration as NativeSnapshots
    for path, content in NativeSnapshots(ROOT).prepare(native_builds).items():
        if path in read_only:
            raise ValueError(f'Update native generated filenames in {path} before importing')
        path.write_text(content)
    from initial_migration.configured_native_codegen import Migration as NativeCodegen
    NativeCodegen(ROOT).run(write=True, read_only=read_only)
    prune_published(ROOT, repository_files, write=True, read_only=read_only, consumers={'@clickhouse'} | {
        '@clickhouse_' + name for name in
        ('nuraft', 'librdkafka', 'croaring', 'google_cloud_cpp')
    })
    for path in native_builds:
        before = path.read_text()
        build = Build(ROOT, path, native_packages.files, native_packages.packages, before)
        files = PackageFiles(build.inventory)
        after = source_globs(before, files, collections.Counter(), [])
        if after != before:
            path.write_text(format_and_prove(ROOT, before, after, str(path), files, {}))
    prune_native_exports(ROOT, write=True)
    from initial_migration.library_runtime import normalize as normalize_library_runtime
    # Pruning must see direct runtime dependencies before ch_cc_library supplies
    # them. Keep the same order on regeneration, after native packages exist.
    for path in native_builds:
        package = path.parent.relative_to(ROOT).as_posix().removeprefix('.')
        if path.name == 'BUILD.bazel' and (not package or native_directory(package)):
            before = path.read_text()
            after = normalize(before, native=True)
            prove(before, after, path)
            after = normalize_library_runtime(after, package)
            if after != before:
                path.write_text(after)
    for spec in REPOSITORIES.values():
        path = ROOT / 'bazel/dependencies' / (spec['name'] + '.BUILD.bazel')
        if path.is_file() and path not in read_only:
            before = path.read_text()
            after = normalize_library_runtime(before, '')
            if after != before:
                path.write_text(after)
    from initial_migration.base_cpp_dependencies import publish_redis_sdk, run as publish_base_cpp_dependencies
    publish_base_cpp_dependencies(ROOT, from_generator=True)
    from initial_migration.web_resources import run as publish_web_resources
    publish_web_resources(ROOT, write=True)
    from initial_migration.prune_native_cpp_inputs import run as prune_native_cpp_inputs
    prune_native_cpp_inputs(ROOT, write=True)
    # The compiler input scan needs the original .cpp labels to inspect each translation unit.
    from initial_migration.first_party_packages import group_runtime_filter_benchmark_sources
    group_runtime_filter_benchmark_sources(ROOT)
    from initial_migration.native_proto_owners import transfer as publish_native_protos
    publish_native_protos(ROOT, write=True)
    # Publishing base moves its BUILD file. Resolve globs against the final
    # package boundaries and retain the exact order of every original file.
    from initial_migration.first_party_packages import git_inventory
    header_inventory = git_inventory(ROOT)
    build_paths = {
        ROOT / name for name in subprocess.check_output(
            ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard',
             '--', 'BUILD', 'BUILD.bazel', '**/BUILD', '**/BUILD.bazel'], cwd=ROOT,
        ).decode().split('\0')
        if name and (ROOT / name).is_file()
    }
    final_packages = {
        path.parent.relative_to(ROOT).as_posix().removeprefix('.') for path in build_paths
    }
    for path in sorted(build_paths):
        package = path.parent.relative_to(ROOT).as_posix().removeprefix('.')
        if not package or native_directory(package):
            before = path.read_text()
            build = Build(ROOT, path, header_inventory, final_packages, before)
            files = PackageFiles(build.inventory)
            after = source_globs(before, files, collections.Counter(), [], headers=True)
            if after != before:
                path.write_text(format_and_prove(ROOT, before, after, str(path), files, {}, headers=True))
    from initial_migration.native_include_deps import run as prune_native_include_deps
    prune_native_include_deps(ROOT, write=True, read_only=read_only)
    from initial_migration.measured_deps import apply as apply_measured_dependencies
    apply_measured_dependencies(ROOT)
    format_build_files(read_only=read_only)
    # Factor formatted lists last: earlier normalization folds equal CPU
    # branches and would discard required no-match errors.
    from initial_migration.factor_build_selections import (
        fold_single_cpu_lists, remove_parens, run as factor_build_selections, source_list_key,
    )
    selection_builds = {
        path for path in build_paths
        if not (package := path.parent.relative_to(ROOT).as_posix().removeprefix('.'))
        or native_directory(package)
    }
    selection_builds.update(
        ROOT / 'bazel/dependencies' / (spec['name'] + '.BUILD.bazel')
        for spec in REPOSITORIES.values()
    )
    selection_builds.difference_update(read_only)
    factor_build_selections(ROOT, selection_builds)
    declare_compiled_parser_headers(ROOT / "src/Parsers/BUILD.bazel")
    declare_unit_test_private_hdrs(ROOT / "src/BUILD.bazel")
    from initial_migration.openldap import declare_openldap
    declare_openldap(ROOT, {arch: graph.targets for arch, graph in graphs.items()})
    from initial_migration.mongo_c import declare_mongo_c_headers
    declare_mongo_c_headers(ROOT / 'bazel/dependencies/clickhouse_mongo_c_driver.BUILD.bazel')
    from initial_migration.native_include_deps import declare_nats_and_mongoc_includes
    declare_nats_and_mongoc_includes(ROOT)
    declare_first_party_header_deps(ROOT)
    from initial_migration.object_storage_headers import apply as declare_object_storage_headers
    declare_object_storage_headers(ROOT)
    from initial_migration.wasm_dictionary_headers import apply as declare_wasm_dictionary_headers
    declare_wasm_dictionary_headers(ROOT)
    declare_external_compiler_headers(ROOT)
    from initial_migration.shared_compile_defines import omit_compile_defaults, share_local_defines
    # Fold remaining CPU choices only when target_compatible_with fixes the CPU.
    # Rule transfers can also leave adjacent literal lists or empty attributes.
    from initial_migration.compact_build import Source, remove_unused_constants, simplify_list_expressions
    for path in sorted(selection_builds):
        before = path.read_text()
        after = before
        while True:
            folded = fold_single_cpu_lists(Source(after))
            updated = simplify_list_expressions(Source(folded), preserve_selects=True)
            if updated == after:
                break
            after = updated
        if after != before:
            path.write_text(after)
    # Fold CPU selections before replacing their literal lists with named definitions.
    share_local_defines(ROOT, selection_builds)
    for path in sorted(selection_builds):
        before = path.read_text()
        after = omit_compile_defaults(Source(drop_defaults(Source(before))))
        after = remove_parens(remove_unused_constants(Source(after)), key=source_list_key)
        if after != before:
            path.write_text(after)
    publish_redis_sdk(ROOT)
    llvm_targets.fold_outline_atomics(ROOT / 'bazel/dependencies/clickhouse_llvm_project.BUILD.bazel')
    from initial_migration.native_compile_options import shared_native_compile_options
    shared_native_compile_options(ROOT, selection_builds)
    from initial_migration.prune_redundant_deps import prune_aggregate_deps
    prune_aggregate_deps(ROOT, build_paths | selection_builds | read_only)
    privatize_headerless_deps(ROOT, [
        ("programs", "clickhouse__compile_0"),
        ("programs/client", "clickhouse-client-lib"),
        ("programs/disks", "clickhouse-disks-lib"),
        ("programs/format", "clickhouse-format-lib"),
        ("programs/server", "clickhouse-server-lib"),
        ("src/AggregateFunctions", "clickhouse_aggregate_functions__compile_0"),
        ("src/AggregateFunctions", "clickhouse_aggregate_functions__compile_1"),
        ("src/Common", "clickhouse_new_delete"),
        ("src/Common/ZooKeeper", "clickhouse_common_zookeeper_no_log"),
        ("src/Daemon", "daemon"),
        ("src/Functions/URL", "clickhouse_functions_url__compile_0"),
    ])
    from initial_migration.first_party_packages import group_package_sources, without_unused_program_include
    group_package_sources(ROOT)
    from initial_migration.package_source_groups import group_owned_sources
    from initial_migration.package_source_globs import compact_owned_source_globs, join_source_globs
    group_owned_sources(ROOT, build_paths, header_inventory, final_packages)
    compact_owned_source_globs(ROOT, build_paths)
    for path in sorted(selection_builds):
        if path.is_file():
            before = path.read_text()
            after = join_source_globs(before)
            if path.parent.parent == ROOT / "programs":
                build = Build(ROOT, path, header_inventory, (), after)
                after = without_unused_program_include(build, native_packages.textual_names)
            if after != before:
                path.write_text(after)
    # Consumers and source owners are final after the BUILD rule rewrites.
    prune_native_exports(ROOT, write=True)
    llvm_targets.fold_component_libraries(ROOT)
    format_build_files(read_only=read_only)
    for path, original in maintained.items():
        if path.read_bytes() != original:
            raise ValueError(f'The CMake importer modified {path}')
    for arch, inventory in inventories.items():
        print(arch, inventory['native_targets'], 'native targets,', inventory['compile_actions'], 'compile actions')


if __name__ == '__main__':
    main()

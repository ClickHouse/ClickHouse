"""Check direct, unused and private C++ dependencies of the requested targets."""

load("@depend_on_what_you_use//dwyu/cc:defs.bzl", "dwyu_cc_aspect_factory", "dwyu_cc_aspect_impl", "dwyu_scan_archive", _dwyu_cc_consumer_audit = "dwyu_cc_consumer_audit")
load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")
load(":llvm_runtime.bzl", "LlvmRuntimeCompileInfo")

# A rust_archive owns one processed static library. Share its llvm-nm output
# at the cc_import that selects that archive, including for transitive callers.
_RustArchiveScans = provider(fields = ["is_rust_archive", "scans"])

def _rust_archive_scans_impl(target, ctx):
    if CcInfo not in target:
        return [_RustArchiveScans(is_rust_archive = ctx.rule.kind == "rust_archive", scans = depset())]
    scans = []
    if ctx.rule.kind == "cc_import":
        producer = getattr(ctx.rule.attr, "static_library", None)
        if producer and _RustArchiveScans in producer and producer[_RustArchiveScans].is_rust_archive:
            archives = ctx.rule.files.static_library
            if len(archives) == 1 and archives[0].owner == producer.label:
                scans.append(dwyu_scan_archive(ctx, archives[0], ctx.label.name + ".dwyu_archive"))
    deps = getattr(ctx.rule.attr, "deps", []) + getattr(ctx.rule.attr, "implementation_deps", [])
    return [_RustArchiveScans(
        is_rust_archive = False,
        scans = depset(scans, transitive = [dep[_RustArchiveScans].scans for dep in deps if _RustArchiveScans in dep]),
    )]

_ARCHIVE_NM = Label("@llvm//tools:llvm-nm")
_rust_archive_scans = aspect(
    implementation = _rust_archive_scans_impl,
    attr_aspects = ["deps", "implementation_deps", "static_library"],
    attrs = {"_tool_nm": attr.label(default = _ARCHIVE_NM, executable = True, allow_files = True, cfg = "exec")},
    provides = [_RustArchiveScans],
)

# CPP callers whose original preprocessing and object symbols inspect Rust FFIs.
_RUST_CONSUMERS = {
    Label("//bazel/rust:chdig_cc"): ("//programs:clickhouse__compile_0", "programs/main.cpp"),
    Label("//bazel/rust:delta_kernel_ffi_cc"): ("//src:dbms__compile_0", "src/Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.cpp"),
    Label("//bazel/rust:pco_cc"): ("//src/Compression:clickhouse_compression", "src/Compression/CompressionCodecPco.cpp"),
    Label("//bazel/rust:polyglot_cc"): ("//src/Parsers:clickhouse_parsers", "src/Parsers/Polyglot/ParserPolyglotQuery.cpp"),
    Label("//bazel/rust:prql_cc"): ("//src/Parsers:clickhouse_parsers", "src/Parsers/PRQL/ParserPRQLQuery.cpp"),
    Label("//bazel/rust:skim_cc"): ("//src:dbms__compile_0", "src/Client/ReplxxLineReader.cpp"),
}

def clickhouse_dwyu_impl(target, ctx):
    producer = target[LlvmRuntimeCompileInfo] if LlvmRuntimeCompileInfo in target else None
    return dwyu_cc_aspect_impl(
        target,
        ctx,
        producer = producer,
        archive_scans = target[_RustArchiveScans].scans if _RustArchiveScans in target else None,
        import_metadata = target.label in _RUST_CONSUMERS,
        import_sources = {
            label: src
            for label, (consumer, src) in _RUST_CONSUMERS.items()
            if target.label == Label(consumer)
        },
    )

dwyu = dwyu_cc_aspect_factory(
    additional_providers = [LlvmRuntimeCompileInfo],
    implementation = clickhouse_dwyu_impl,
    tool_analyze_includes = Label("//bazel/rules:dwyu_analyze_includes"),
    tool_preprocessing = Label("//bazel/rules:dwyu_preprocessing"),
    tool_process_target = Label("//bazel/rules:dwyu_process_target"),
    tool_nm = _ARCHIVE_NM,
    required_aspects = [_rust_archive_scans],
    analysis_optimizes_impl_deps = True,
    preprocessing_mode = "compiler",
    tool_import_evidence = Label("//bazel/initial_migration:rust_import_evidence"),
)

# Dev-only closure audit. Unsupported rules forward descendant reports but
# never receive a successful C++ report themselves.
dwyu_closure = dwyu_cc_aspect_factory(
    implementation = clickhouse_dwyu_impl,
    tool_analyze_includes = Label("//bazel/rules:dwyu_analyze_includes"),
    tool_preprocessing = Label("//bazel/rules:dwyu_preprocessing"),
    tool_process_target = Label("//bazel/rules:dwyu_process_target"),
    tool_nm = _ARCHIVE_NM,
    required_aspects = [_rust_archive_scans],
    analysis_optimizes_impl_deps = True,
    preprocessing_mode = "compiler",
    recursive = True,
    follow_configured_closure = True,
)

# The production census supplies the exact repositories from verified BUILD metadata.
dwyu_production_closure = dwyu_cc_aspect_factory(
    implementation = clickhouse_dwyu_impl,
    tool_analyze_includes = Label("//bazel/rules:dwyu_analyze_includes"),
    tool_preprocessing = Label("//bazel/rules:dwyu_preprocessing"),
    tool_process_target = Label("//bazel/rules:dwyu_process_target"),
    tool_nm = _ARCHIVE_NM,
    required_aspects = [_rust_archive_scans],
    tool_import_evidence = Label("//bazel/initial_migration:rust_import_evidence"),
    analysis_optimizes_impl_deps = True,
    preprocessing_mode = "compiler",
    recursive = True,
    follow_configured_closure = True,
    production_repositories = Label("//bazel/rules:dwyu_production_repositories"),
)

# Dev-only rule: analyze textual headers in actual configured consumers.
dwyu_cc_consumer_audit = _dwyu_cc_consumer_audit

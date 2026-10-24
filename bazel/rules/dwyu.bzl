"""Check direct, unused and private C++ dependencies of the requested targets."""

load("@depend_on_what_you_use//dwyu/cc:defs.bzl", "dwyu_cc_aspect_factory", "dwyu_cc_aspect_impl", _dwyu_cc_consumer_audit = "dwyu_cc_consumer_audit")
load(":llvm_runtime.bzl", "LlvmRuntimeCompileInfo")

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
    tool_nm = Label("@llvm//tools:llvm-nm"),
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
    tool_nm = Label("@llvm//tools:llvm-nm"),
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
    tool_nm = Label("@llvm//tools:llvm-nm"),
    tool_import_evidence = Label("//bazel/initial_migration:rust_import_evidence"),
    analysis_optimizes_impl_deps = True,
    preprocessing_mode = "compiler",
    recursive = True,
    follow_configured_closure = True,
    production_repositories = Label("//bazel/rules:dwyu_production_repositories"),
)

# Dev-only rule: analyze textual headers in actual configured consumers.
dwyu_cc_consumer_audit = _dwyu_cc_consumer_audit

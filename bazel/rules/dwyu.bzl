"""Check direct, unused and private C++ dependencies of the requested targets."""

load("@depend_on_what_you_use//dwyu/cc:defs.bzl", "dwyu_cc_aspect_factory", "dwyu_cc_aspect_impl", _dwyu_cc_consumer_audit = "dwyu_cc_consumer_audit")
load(":llvm_runtime.bzl", "LlvmRuntimeCompileInfo")

def clickhouse_dwyu_impl(target, ctx):
    producer = target[LlvmRuntimeCompileInfo] if LlvmRuntimeCompileInfo in target else None
    return dwyu_cc_aspect_impl(target, ctx, producer = producer)

dwyu = dwyu_cc_aspect_factory(
    additional_providers = [LlvmRuntimeCompileInfo],
    implementation = clickhouse_dwyu_impl,
    tool_analyze_includes = Label("//bazel/rules:dwyu_analyze_includes"),
    tool_preprocessing = Label("//bazel/rules:dwyu_preprocessing"),
    tool_process_target = Label("//bazel/rules:dwyu_process_target"),
    tool_nm = Label("@llvm//tools:llvm-nm"),
    analysis_optimizes_impl_deps = True,
    preprocessing_mode = "compiler",
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
    analysis_optimizes_impl_deps = True,
    preprocessing_mode = "compiler",
    recursive = True,
    follow_configured_closure = True,
    production_repositories = Label("//bazel/rules:dwyu_production_repositories"),
)

# Dev-only rule: analyze textual headers in actual configured consumers.
dwyu_cc_consumer_audit = _dwyu_cc_consumer_audit

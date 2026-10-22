"""Exercise failure of the actual runtime object symbol action."""

load("@depend_on_what_you_use//dwyu/cc:defs.bzl", "dwyu_cc_aspect_factory")
load("//bazel/rules:dwyu.bzl", "clickhouse_dwyu_impl")
load("//bazel/rules:llvm_runtime.bzl", "LlvmRuntimeCompileInfo")

def _failing_nm_impl(ctx):
    exe = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.write(exe, "#!/bin/sh\nexit 53\n", is_executable = True)
    return [DefaultInfo(executable = exe)]

failing_nm = rule(
    implementation = _failing_nm_impl,
    executable = True,
)

nm_failure = dwyu_cc_aspect_factory(
    additional_providers = [LlvmRuntimeCompileInfo],
    implementation = clickhouse_dwyu_impl,
    tool_analyze_includes = Label("//bazel/rules:dwyu_analyze_includes"),
    tool_preprocessing = Label("//bazel/rules:dwyu_preprocessing"),
    tool_process_target = Label("//bazel/rules:dwyu_process_target"),
    tool_nm = Label("//bazel/initial_migration/dwyu_regressions/runtime_compile:failing_nm"),
    analysis_optimizes_impl_deps = True,
    preprocessing_mode = "compiler",
)

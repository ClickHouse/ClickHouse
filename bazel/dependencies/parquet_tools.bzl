"""Build the command-line programs in Arrow's cpp/tools/parquet directory."""

load("//bazel/rules:ch_cc_library.bzl", "ch_cc_binary")
load("//bazel/rules:native_compile_options.bzl", "NATIVE_TOOL_LINKOPTS", "native_cxx_tool_copts")

def parquet_cc_binary(name, srcs, local_defines):
    """Link a Parquet tool against Arrow and Parquet."""
    ch_cc_binary(
        name = name,
        srcs = srcs,
        copts = native_cxx_tool_copts("-std=c++23"),
        features = [
            "-static_link_cpp_runtimes",
            "-fortify_source",
            "-llvm_system_c_headers",
        ],
        linkopts = NATIVE_TOOL_LINKOPTS + ["-lpthread"],
        local_defines = local_defines,
        deps = [
            "@clickhouse_llvm_project//:clang_rt_xray",
            "@clickhouse_llvm_project//:libllvmlibc",
            ":_arrow",
            ":_parquet",
            "@clickhouse_zstd//:_zstd",
            "@clickhouse_llvm_project//:cxx",
            "@clickhouse_llvm_project//:cxxabi",
            "@clickhouse//base/glibc-compatibility",
            "@clickhouse_llvm_project//:unwind",
        ],
    )

"""Build the command-line programs in Arrow's cpp/tools/parquet directory."""

load("//bazel/rules:ch_cc_library.bzl", "ch_cc_binary")
load("//bazel/rules:native_compile_options.bzl", "NATIVE_CODEGEN_COPTS", "native_cxx_tool_copts")

_PARQUET_LINKOPTS = [
    "--no-default-config",
    "-fdiagnostics-color=always",
    "-pipe",
] + NATIVE_CODEGEN_COPTS + [
    "-fdiagnostics-absolute-paths",
    "-fstrict-vtable-pointers",
    "-fPIE",
    "-D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_FAST",
    "-w",
    "-ffunction-sections",
    "-fdata-sections",
    "-faddrsig",
    "-O3",
    "-DNDEBUG",
    "--no-default-config",
    "-Wl,--no-export-dynamic",
    "-Wl,--gc-sections",
    "-Wl,--fatal-warnings",
    "-Wl,--icf=safe",
    "-Wl,--strip-debug",
    "-Wl,--build-id=sha1",
    "-Wl,-u,logf",
    "-Wl,-u,powf",
    "-Wl,-u,expf",
    "-Wl,-u,exp2f",
    "-Wl,-u,log2f",
    "-Wl,-u,log",
    "-Wl,-u,pow",
    "-Wl,-u,exp",
    "-Wl,-u,exp2",
    "-Wl,-u,log2",
    "-Wl,-u,lgammaf",
    "-pie",
    "-Wl,-pie",
    "-fno-xray-link-deps",
    "-Wl,-u,memcpy",
    "-Wl,-u,memmove",
    "-Wl,-u,memset",
    "-Wl,-u,memcmp",
    "-Wl,-z,now",
    "-Wl,-z,relro",
    "-Wl,-z,separate-code",
    "-Wl,-z,keep-text-section-prefix",
    "-Xlinker",
    "--no-undefined",
    "-lpthread",
]

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
        linkopts = _PARQUET_LINKOPTS,
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

"""Clang options for ClickHouse's native tools, unit_tests_dbms, and LLVM runtime sources."""

load("//bazel/rules:ch_cc_library.bzl", "CLICKHOUSE_CODE_FLAGS")
load("//bazel/rules:repository_paths.bzl", "CLICKHOUSE_WARNINGS", "cmake_prefix_maps")

# Preserve CMake's position of -mbranches-within-32B-boundaries after the section flags.
NATIVE_CODEGEN_COPTS = select({
    "@platforms//cpu:x86_64": ["-march=x86-64-v3", "-mpclmul"],
    "@platforms//cpu:aarch64": ["-march=armv8.2-a+simd+crypto+dotprod+ssbs+rcpc+bf16"],
}) + [
    "-fasynchronous-unwind-tables",
    "-fdebug-compilation-dir=.",
    "-falign-functions=64",
    "-falign-loops=32",
    "-ffunction-sections",
    "-fdata-sections",
    "-faddrsig",
] + select({
    "@platforms//cpu:x86_64": ["-mbranches-within-32B-boundaries"],
    "@platforms//cpu:aarch64": [],
}) + [
    "-fstack-protector-strong",
    "-fstack-clash-protection",
    "-ffp-contract=off",
    "-fno-math-errno",
]

_NATIVE_TOOL_DRIVER_FLAGS = [
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
]

NATIVE_TOOL_LINKOPTS = _NATIVE_TOOL_DRIVER_FLAGS + [
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
]

# CMake passes the same Clang options when compiling and linking unit_tests_dbms.
DBMS_TEST_DRIVER_COPTS = [
    "--no-default-config",
    "-fdiagnostics-color=always",
    "-pipe",
] + NATIVE_CODEGEN_COPTS + [
    "-fdiagnostics-absolute-paths",
    "-fstrict-vtable-pointers",
    "-fPIE",
    "-D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_FAST",
] + CLICKHOUSE_CODE_FLAGS + ["-Wframe-larger-than=65536"] + CLICKHOUSE_WARNINGS + ["-O3", "-DNDEBUG"]

def native_release_copts(standard = None):
    """Return the Linux release flags, with the language standard in CMake order."""
    return ["-O3", "-DNDEBUG"] + ([standard] if standard else []) + ["-D", "OS_LINUX", "-gz=zlib"]

def native_cxx_tool_copts(standard):
    """Return the complete copts for flatc, protoc, grpc_cpp_plugin and the Parquet tools."""
    return _NATIVE_TOOL_DRIVER_FLAGS + native_release_copts(standard) + ["-Werror"] + cmake_prefix_maps(["aarch64", "x86_64"])

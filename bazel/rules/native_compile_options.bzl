"""Clang options for ClickHouse's native build tools and LLVM runtime sources."""

load("//bazel/rules:repository_paths.bzl", "cmake_prefix_maps")

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

def native_release_copts(standard = None):
    """Return the Linux release flags, with the language standard in CMake order."""
    return ["-O3", "-DNDEBUG"] + ([standard] if standard else []) + ["-D", "OS_LINUX", "-gz=zlib"]

def native_cxx_tool_copts(standard):
    """Return the complete copts for flatc, protoc, grpc_cpp_plugin and the Parquet tools."""
    return [
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
    ] + native_release_copts(standard) + [
        "-Werror",
        "-nostdinc++",
    ] + cmake_prefix_maps(["aarch64", "x86_64"])

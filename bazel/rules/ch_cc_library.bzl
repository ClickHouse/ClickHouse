"""CMake native libraries compiled for the ClickHouse target platform."""

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")
load(":repository_paths.bzl", "CLICKHOUSE_WARNINGS", "cmake_prefix_maps", "repository_path")

# Place these before library copts: several libraries require an older ISA.
CLICKHOUSE_TARGET_FLAGS = {
    "x86_64": ["-march=x86-64-v3", "-mpclmul", "-mbranches-within-32B-boundaries"],
    "aarch64": ["-march=armv8.2-a+simd+crypto+dotprod+ssbs+rcpc+bf16"],
}

CLICKHOUSE_CODE_FLAGS = [
    "-fxray-instrument",
    "-fxray-modes=none",
    "-fxray-instrumentation-bundle=function",
    "-Wall",
    "-Wextra",
]

def ch_cc_library(
        name,
        hdrs = [],
        copts = [],
        clickhouse_code = False,
        source_headers = False,
        language = "CXX",
        arches = ["x86_64", "aarch64"],
        libcxx_headers = True,
        llvm_headers = False,
        link_runtime = False,
        map_source_files = True,
        features = [],
        **kwargs):
    """Compile a CMake library, with CPU defaults before source overrides.

    llvm_headers supplies LLVM compiler-rt, libc and libunwind headers for
    libraries which request them. libcxx_headers supplies the pinned libc++
    headers to C/ASM. link_runtime privately supplies libc++; its dependencies
    provide libc++abi and libunwind.
    source_headers adds //src:clickhouse_source_headers privately when True.
    """
    cpu_flags = select({
        str(Label("@platforms//cpu:" + arch)): flags
        for arch, flags in CLICKHOUSE_TARGET_FLAGS.items()
    } | {"//conditions:default": []})
    code_flags = CLICKHOUSE_CODE_FLAGS if clickhouse_code else []
    if source_headers:
        headers = [Label("@clickhouse//src:clickhouse_source_headers")]
        kwargs["implementation_deps"] = kwargs.get("implementation_deps", []) + select({
            str(Label("@platforms//cpu:" + arch)): headers
            for arch in arches
        } | {"//conditions:default": []})
    if clickhouse_code and language == "CXX":
        defaults = ["-Wframe-larger-than=65536"] + CLICKHOUSE_WARNINGS + ["-std=c++23"]
        code_flags = code_flags + select({
            str(Label("@platforms//cpu:" + arch)): defaults if arch in arches else []
            for arch in CLICKHOUSE_TARGET_FLAGS
        } | {"//conditions:default": defaults})
    if llvm_headers:
        headers = [
            Label("@llvm-project//compiler-rt:headers"),
            Label("@llvm-project//libc:libc_headers"),
            Label("@llvm-project//libunwind:headers"),
        ]
        kwargs["deps"] = select({
            str(Label("@platforms//cpu:" + arch)): headers if arch in arches else []
            for arch in CLICKHOUSE_TARGET_FLAGS
        } | {"//conditions:default": headers}) + kwargs.get("deps", [])
    if link_runtime:
        runtime = [Label("@clickhouse_llvm_project//:cxx")]
        kwargs["implementation_deps"] = kwargs.get("implementation_deps", []) + select({
            str(Label("@platforms//cpu:" + arch)): runtime if arch in arches else []
            for arch in CLICKHOUSE_TARGET_FLAGS
        } | {"//conditions:default": runtime})
    if language in ["C", "ASM"] and libcxx_headers:
        kwargs["implementation_deps"] = select({
            str(Label("@platforms//cpu:" + arch)): [Label("@clickhouse_llvm_project//:libcxx_config_headers")]
            for arch in arches
        } | {"//conditions:default": []}) + kwargs.get("implementation_deps", [])
    cc_library(
        name = name,
        hdrs = hdrs,
        copts = cpu_flags + code_flags + copts +
                (cmake_prefix_maps(arches) if map_source_files else []) + _sysroot_copts(arches, language),
        features = ["-fortify_source", "-default_compile_flags", "-llvm_system_c_headers"] + features,
        **kwargs
    )

def _sysroot_copts(arches, language):
    native_options = {"//conditions:default": []}
    for arch in arches:
        cpu = str(Label("@platforms//cpu:" + arch))
        gcc_root = repository_path("contrib/sysroot") + "/linux-" + arch
        options = [
            "--sysroot=" + gcc_root + "/" + arch + "-linux-gnu/libc",
            "--gcc-toolchain=" + gcc_root,
        ]
        if language == "CXX":
            # GCC's sysroot must not supply a second C++ standard library.
            options.append("-nostdinc++")
        native_options[cpu] = options
    return select(native_options)

def ch_cc_binary(name, copts = [], **kwargs):
    """Build a C++ tool with the same sysroot as ch_cc_library."""
    cc_binary(
        name = name,
        copts = copts + _sysroot_copts(["x86_64", "aarch64"], "CXX"),
        **kwargs
    )

def ch_cc_test(name, copts = [], **kwargs):
    """Build a C++ test with the same sysroot as ch_cc_library."""
    cc_test(
        name = name,
        copts = copts + _sysroot_copts(["x86_64", "aarch64"], "CXX"),
        **kwargs
    )

"""CMake native libraries compiled for the ClickHouse target platform."""

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load(":repository_paths.bzl", "CLICKHOUSE_WARNINGS", "cmake_prefix_maps")

# Public compile definitions of the ClickHouse libc++, libc++abi, and libunwind.
CXX_RUNTIME_DEFINES = [
    "STD_EXCEPTION_HAS_STACK_TRACE=1",
    "_LIBCPP_ENABLE_THREAD_SAFETY_ANNOTATIONS",
    "_LIBCPP_REMOVE_TRANSITIVE_INCLUDES=1",
    "_LIBUNWIND_IS_NATIVE_ONLY",
]

# Place these before library copts: several libraries require an older ISA.
CLICKHOUSE_TARGET_FLAGS = {
    "x86_64": ["-march=x86-64-v3", "-mpclmul", "-mbranches-within-32B-boundaries"],
    "aarch64": ["-march=armv8.2-a+simd+crypto+dotprod+ssbs+rcpc+bf16"],
}

CLICKHOUSE_XRAY_FLAGS = [
    "-fxray-instrument",
    "-fxray-modes=none",
    "-fxray-instrumentation-bundle=function",
]

_CLICKHOUSE_CXX_WARNINGS = ["-Wall", "-Wextra"]
CLICKHOUSE_CODE_FLAGS = CLICKHOUSE_XRAY_FLAGS + _CLICKHOUSE_CXX_WARNINGS

def _for_arches(arches, values):
    """Return values on enabled CPUs and the default platform."""
    if all([arch in arches for arch in CLICKHOUSE_TARGET_FLAGS]):
        return values
    return select({
        str(Label("@platforms//cpu:" + arch)): values if arch in arches else []
        for arch in CLICKHOUSE_TARGET_FLAGS
    } | {"//conditions:default": values})

def ch_cc_library(
        name,
        hdrs = [],
        copts = None,
        clickhouse_code = False,
        language = "CXX",
        arches = ["x86_64", "aarch64"],
        link_runtime = False,
        map_source_files = True,
        features = [],
        **kwargs):
    """Compile a CMake library, with CPU defaults before source overrides.

    link_runtime adds libc++ privately; libc++ supplies libc++abi and libunwind.
    Compiled C++ libraries use CXX_RUNTIME_DEFINES unless local_defines is set.
    Third-party C and C++ libraries default to -w and the ClickHouse language
    standard. An explicit copts list replaces both flags, including when empty.
    """
    if copts == None:
        copts = []
        if kwargs.get("srcs") and not clickhouse_code and language in ("C", "CXX"):
            copts = ["-w", "-std=gnu11" if language == "C" else "-std=c++23"]
    if kwargs.get("srcs") and language == "CXX":
        kwargs.setdefault("local_defines", CXX_RUNTIME_DEFINES)
    cpu_flags = select({
        str(Label("@platforms//cpu:" + arch)): flags
        for arch, flags in CLICKHOUSE_TARGET_FLAGS.items()
    } | {"//conditions:default": []})
    code_flags = []
    if clickhouse_code:
        code_flags = CLICKHOUSE_XRAY_FLAGS if language == "C" else CLICKHOUSE_CODE_FLAGS
    if clickhouse_code and language in ["C", "CXX"]:
        standard = "-std=gnu11" if language == "C" else "-std=c++23"
        defaults = ["-Wframe-larger-than=65536"] + CLICKHOUSE_WARNINGS + [standard]
        code_flags = code_flags + _for_arches(arches, defaults)
    if link_runtime:
        runtime = [Label("@clickhouse_llvm_project//:cxx")]
        kwargs["implementation_deps"] = kwargs.get("implementation_deps", []) + _for_arches(arches, runtime)
    cc_library(
        name = name,
        hdrs = hdrs,
        copts = cpu_flags + code_flags + copts +
                (cmake_prefix_maps(arches) if map_source_files else []),
        features = ["-fortify_source", "-default_compile_flags"] + features,
        **kwargs
    )

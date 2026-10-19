"""CMake native libraries compiled for the ClickHouse target platform."""

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load(":repository_paths.bzl", "cmake_prefix_maps", "repository_path")

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
        additional_compiler_inputs = [],
        copts = [],
        clickhouse_code = False,
        language = "CXX",
        arches = ["x86_64", "aarch64"],
        libcxx_headers = True,
        map_source_files = True,
        features = [],
        **kwargs):
    """Keep CPU defaults ahead of per-source ISA and warning overrides."""
    cpu_flags = select({
        str(Label("@platforms//cpu:" + arch)): flags
        for arch, flags in CLICKHOUSE_TARGET_FLAGS.items()
    } | {"//conditions:default": []})
    native_options = {"//conditions:default": []}
    sysroot_headers = {"//conditions:default": []}
    sysroot_files = {"//conditions:default": []}
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
        elif language in ["C", "ASM"] and libcxx_headers:
            options.extend([
                "-I",
                "$(GENDIR)/" + repository_path("contrib/llvm-project") +
                "/gen/libcxx/include/c++/v1",
            ])
        native_options[cpu] = options
        sysroot_headers[cpu] = [Label("@clickhouse_sysroot//:linux_native_headers")]
        sysroot_files[cpu] = [Label("@clickhouse_sysroot//:linux_compile_files")]
    cc_library(
        name = name,
        hdrs = hdrs + select(sysroot_headers),
        additional_compiler_inputs = additional_compiler_inputs + select(sysroot_files),
        copts = cpu_flags + (CLICKHOUSE_CODE_FLAGS if clickhouse_code else []) + copts +
                (cmake_prefix_maps(arches) if map_source_files else []) + select(native_options),
        features = ["-fortify_source", "-default_compile_flags", "-llvm_system_c_headers"] + features,
        **kwargs
    )

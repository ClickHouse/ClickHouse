"""CMake native libraries compiled for the ClickHouse target platform."""

load("@rules_cc//cc:cc_library.bzl", "cc_library")

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

def ch_cc_library(name, copts, clickhouse_code = False, **kwargs):
    """Keep CPU defaults ahead of per-source ISA and warning overrides."""
    cpu_flags = select({
        str(Label("@platforms//cpu:" + arch)): flags
        for arch, flags in CLICKHOUSE_TARGET_FLAGS.items()
    } | {"//conditions:default": []})
    cc_library(
        name = name,
        copts = cpu_flags + (CLICKHOUSE_CODE_FLAGS if clickhouse_code else []) + copts,
        **kwargs
    )

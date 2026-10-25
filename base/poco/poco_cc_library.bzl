"""Poco C and C++ libraries compiled with ClickHouse warnings and instrumentation."""

load("@clickhouse//bazel/rules:ch_cc_library.bzl", "ch_cc_library")
load("@clickhouse//bazel/rules:repository_paths.bzl", "CLICKHOUSE_WARNINGS")

def poco_cc_library(name, copts = [], language = "CXX", **kwargs):
    """Compile Poco, including the bundled C libraries."""
    if language == "C":
        copts = [
            "-fxray-instrument",
            "-fxray-modes=none",
            "-fxray-instrumentation-bundle=function",
            "-Wframe-larger-than=65536",
        ] + CLICKHOUSE_WARNINGS + ["-std=gnu11"] + copts
        kwargs.setdefault("llvm_headers", True)
    else:
        kwargs["clickhouse_code"] = True

    ch_cc_library(
        name = name,
        copts = copts,
        language = language,
        **kwargs
    )

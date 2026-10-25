"""Poco C and C++ libraries compiled with ClickHouse warnings and instrumentation."""

load("@clickhouse//bazel/rules:ch_cc_library.bzl", "ch_cc_library")

def poco_cc_library(name, copts = [], language = "CXX", **kwargs):
    """Compile Poco, including the bundled C libraries."""
    if language == "C":
        kwargs.setdefault("llvm_headers", True)

    ch_cc_library(
        name = name,
        copts = copts,
        clickhouse_code = True,
        language = language,
        **kwargs
    )

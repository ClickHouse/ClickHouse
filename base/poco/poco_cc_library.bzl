"""Poco C and C++ libraries compiled with ClickHouse warnings and instrumentation."""

load("@clickhouse//bazel/rules:ch_cc_library.bzl", "ch_cc_library")

def poco_cc_library(name, **kwargs):
    """Compile Poco, including the bundled C libraries."""
    ch_cc_library(
        name = name,
        clickhouse_code = True,
        **kwargs
    )

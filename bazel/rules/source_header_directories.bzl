"""Stage complete, single-owner header directories from pinned external repositories."""

load("@llvm//:directory.bzl", "headers_directory")

def source_header_directories(paths):
    """Return directory inputs for cc_library while preserving strict include checks."""
    labels = []
    for path in paths:
        name = "_source_headers/" + path
        headers_directory(name = name, path = path, visibility = ["//visibility:private"])
        labels.append(":" + name)

    # Bazel validates each file in Clang's dependency file when strict system
    # includes are enabled. Keep the original file inputs in that configuration.
    return select({
        "@clickhouse//bazel:strict_system_includes": [],
        "//conditions:default": labels,
    })

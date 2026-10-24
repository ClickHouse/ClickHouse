"""Compiler definitions of the SDK and CRT sources compiled by contrib/aws-cmake."""

load("@clickhouse//bazel/rules:ch_cc_library.bzl", "CXX_RUNTIME_DEFINES")

_AWS_DEFINES = CXX_RUNTIME_DEFINES + [
    "AWS_AFFINITY_METHOD=AWS_AFFINITY_METHOD_NONE",
    "AWS_ENABLE_EPOLL",
    "AWS_PTHREAD_SETNAME_TAKES_2ARGS",
    "ENABLE_OPENSSL_ENCRYPTION",
    "INTEL_NO_ITTNOTIFY_API",
]

AWS_CRT_DEFINES = select({
    "@platforms//cpu:x86_64": sorted(_AWS_DEFINES + ["USE_SIMD_ENCODING"]),
    "@platforms//cpu:aarch64": sorted(_AWS_DEFINES),
})

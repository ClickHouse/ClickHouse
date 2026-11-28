"""Compile LLVM component libraries with the ClickHouse LLVM configuration."""

load(":ch_cc_library.bzl", "ch_cc_library")

LLVM_DEFINES = select({
    "@platforms//cpu:x86_64": [],
    "@platforms//cpu:aarch64": [
        "BLAKE3_NO_AVX2",
        "BLAKE3_NO_AVX512",
        "BLAKE3_NO_SSE2",
        "BLAKE3_NO_SSE41",
    ],
}) + [
    "LLVM_ENABLE_EH=1",
    "LLVM_ENABLE_RTTI=1",
    "LLVM_ENABLE_THREADS=1",
    "LLVM_HAS_ATOMICS=1",
    "STD_EXCEPTION_HAS_STACK_TRACE=1",
    "_LARGEFILE64_SOURCE",
    "_LIBCPP_ENABLE_THREAD_SAFETY_ANNOTATIONS",
    "_LIBCPP_REMOVE_TRANSITIVE_INCLUDES=1",
    "_LIBUNWIND_IS_NATIVE_ONLY",
    "__STDC_CONSTANT_MACROS",
    "__STDC_FORMAT_MACROS",
    "__STDC_LIMIT_MACROS",
]

_LLVM_CXX_COPTS = ["-w", "-std=c++17"]

def llvm_cc_library(name, copts = _LLVM_CXX_COPTS, local_defines = LLVM_DEFINES, **kwargs):
    """Compile an LLVM C++ component with LLVM's C++17 options and definitions."""
    ch_cc_library(name = name, copts = copts, local_defines = local_defines, **kwargs)

"""Compile a ZXC compression, decompression, or Huffman implementation."""

load("//bazel/rules:ch_cc_library.bzl", "ch_cc_library")

_ZXC_VECTOR_FLAGS = {
    "avx2": ["-mavx2"],
    "avx512": ["-mavx512f", "-mavx512bw", "-mavx512vbmi", "-mavx512vbmi2"],
}

def zxc_cc_library(name, srcs, variant = "default", **kwargs):
    """Compile one ZXC source with the symbol suffix for its instruction set."""
    if variant != "default":
        kwargs["copts"] = ["-w", "-std=gnu11"] + _ZXC_VECTOR_FLAGS[variant] + ["-mbmi", "-mbmi2", "-mlzcnt"]
    ch_cc_library(
        name = name,
        srcs = srcs,
        alwayslink = True,
        language = "C",
        local_includes = ["include", "src/lib", "src/lib/vendors"],
        local_defines = [
            "STD_EXCEPTION_HAS_STACK_TRACE=1",
            "ZXC_FUNCTION_SUFFIX=_" + variant,
            "ZXC_STATIC_DEFINE",
            "_LIBCPP_ENABLE_THREAD_SAFETY_ANNOTATIONS",
            "_LIBCPP_REMOVE_TRANSITIVE_INCLUDES=1",
            "_LIBUNWIND_IS_NATIVE_ONLY",
        ],
        **kwargs
    )

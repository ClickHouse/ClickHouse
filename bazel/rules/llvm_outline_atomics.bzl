"""Declare the AArch64 compiler-rt outline helpers assembled from lse.S."""

load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load(":llvm_runtime.bzl", "llvm_runtime_objects")

def llvm_outline_atomics(*, arch, copts, headers, local_defines, local_includes, reference_source_root, system_includes):
    """Assemble each operation, operand size and memory model under its CMake compile-group number."""
    arm64 = ["@platforms//cpu:aarch64"]
    group = 2
    for operation in ["cas", "swp", "ldadd", "ldclr", "ldeor", "ldset"]:
        for size in ([1, 2, 4, 8, 16] if operation == "cas" else [1, 2, 4, 8]):
            for model in range(1, 6):
                stem = "outline_atomic_%s%d_%d" % (operation, size, model)
                assembly = "gen/compiler-rt-outline/" + stem + ".S"
                objects = "clang_rt_builtins__runtime_objects_%d" % group
                copy_file(
                    name = "runtime_outline_" + stem + "_S",
                    src = ":compiler-rt/lib/builtins/aarch64/lse.S",
                    out = assembly,
                    target_compatible_with = arm64,
                )
                llvm_runtime_objects(
                    name = objects,
                    srcs = [":" + assembly] + headers,
                    arch = arch,
                    copts = copts,
                    language = "ASM",
                    local_defines = ["L_" + operation, "MODEL=%d" % model, "SIZE=%d" % size] + local_defines,
                    local_includes = local_includes,
                    reference_source_root = reference_source_root,
                    system_includes = system_includes,
                    target_compatible_with = arm64,
                )
                cc_library(
                    name = "clang_rt_builtins__compile_%d" % group,
                    srcs = [":" + objects],
                    alwayslink = True,
                    target_compatible_with = arm64,
                )
                group += 1

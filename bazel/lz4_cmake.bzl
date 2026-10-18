"""Compile the four CMake _lz4 objects used by the x86_64 CMake link."""

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load(":repository_paths.bzl", "cmake_native_compile_flags", "cmake_prefix_maps", "repository_path")

def declare_lz4_cmake_objects():
    # Saved CMake _lz4 compileGroups, also recorded in compile_commands.json.
    # Keep the order used by Plan.make_slots and the CMake static archive.
    cc_library(
        name = "_lz4__compile_0",
        srcs = [
            "lib/lz4.c",
            "lib/lz4hc.c",
            "lib/lz4frame.c",
            "lib/xxhash.c",
        ],
        hdrs = native.glob(["lib/*.h"]) + [
            "@clickhouse_sysroot//:linux_native_headers",
        ],
        additional_compiler_inputs = [
            # lz4hc.c includes lz4.c in addition to the standalone lz4.c object.
            "lib/lz4.c",
            "@clickhouse_sysroot//:linux_compile_files",
            "@clickhouse_llvm_project//:runtime_source_inputs",
            "@clickhouse_llvm_project//:runtime_generated_headers",
        ],
        copts = [
            "--no-default-config",
            "-fdiagnostics-color=always",
            "-pipe",
            "-march=x86-64-v3",
            "-mpclmul",
            "-fasynchronous-unwind-tables",
            "-fdebug-compilation-dir=.",
            "-falign-functions=64",
            "-falign-loops=32",
            "-ffunction-sections",
            "-fdata-sections",
            "-faddrsig",
            "-mbranches-within-32B-boundaries",
            "-fstack-protector-strong",
            "-fstack-clash-protection",
            "-ffp-contract=off",
            "-fno-math-errno",
            "-fdiagnostics-absolute-paths",
            "-fPIE",
            "-w",
            "-ffunction-sections",
            "-fdata-sections",
            "-faddrsig",
            "-O3",
            "-DNDEBUG",
            "-std=gnu11",
            "-D",
            "OS_LINUX",
            "-gz=zlib",
            "-Werror",
            "-isystem",
            repository_path("contrib/lz4") + "/lib",
        ] + cmake_prefix_maps(["x86_64"]) + cmake_native_compile_flags({
            "x86_64": {
                "language": "C",
                "includes": [
                    ["-I", "generated", "libcxx/include/c++/v1"],
                    ["-isystem", "source", "libcxxabi/include"],
                ],
            },
        }),
        local_defines = [
            "LZ4_FAST_DEC_LOOP=1",
            "STD_EXCEPTION_HAS_STACK_TRACE=1",
            "_LIBCPP_ENABLE_THREAD_SAFETY_ANNOTATIONS",
            "_LIBCPP_REMOVE_TRANSITIVE_INCLUDES=1",
            "_LIBUNWIND_IS_NATIVE_ONLY",
        ],
        deps = [
            "@llvm-project//compiler-rt:headers",
            "@llvm-project//libc:libc_headers",
            "@llvm-project//libunwind:headers",
        ],
        features = ["-fortify_source", "-default_compile_flags", "-llvm_system_c_headers"],
        target_compatible_with = ["@platforms//cpu:x86_64", "@platforms//os:linux"],
    )

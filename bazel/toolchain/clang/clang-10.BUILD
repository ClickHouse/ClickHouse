package(default_visibility = ["//visibility:public"])

load("@bazel_tools//tools/cpp:cc_toolchain_config_lib.bzl", "tool_path")

def _impl(ctx):
    tool_paths = [
        tool_path(
            name = "ar",
            path = "bin/llvm-ar",
        ),
        tool_path(
            name = "cpp",
            path = "bin/clang-cpp",
        ),
        tool_path(
            name = "gcc",
            path = "bin/clang",
        ),
        tool_path(
            name = "gcov",
            path = "bin/llvm-cov",
        ),
        tool_path(
            name = "ld",
            path = "bin/ld.lld",
        ),
        tool_path(
            name = "nm",
            path = "bin/llvm-nm",
        ),
        tool_path(
            name = "objdump",
            path = "bin/llvm-objdump",
        ),
        tool_path(
            name = "strip",
            path = "bin/llvm-strip",
        ),
    ]

    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        toolchain_identifier = "clang-amd64-config",
        host_system_name = "unknown",
        target_system_name = "unknown",
        target_cpu = "k8",
        target_libc = "glibc-2.4",
        compiler = "clang-10",
        abi_version = "clang-10",
        abi_libc_version = "glibc-2.4",

        tool_paths = tool_paths,
    )

cc_toolchain_config = rule(
    attrs = {},
    implementation = _impl,
    provides = [CcToolchainConfigInfo],
)

cc_toolchain_config(name = "clang-amd64-config")

filegroup(
    name = "all",
    srcs = [
        ":binutils_components",
        ":compiler_components",
        ":linker_components",
    ],
)

filegroup(
    name = "binutils_components",
    srcs = [
        ":ar",
        ":as",
    ],
)

filegroup(
    name = "compiler_components",
    srcs = [
        ":clang",
        ":include",
    ],
)

filegroup(
    name = "linker_components",
    srcs = [
        ":ar",
        ":clang",
        ":ld",
        ":lib",
    ],
)

filegroup(
    name = "ar",
    srcs = ["bin/llvm-ar"],
)

filegroup(
    name = "as",
    srcs = ["bin/llvm-as"],
)

filegroup(
    name = "clang",
    srcs = [
        "bin/clang",
        "bin/clang++",
    ],
)

filegroup(
    name = "dwp",
    srcs = ["bin/llvm-dwp"],
)

filegroup(
    name = "include",
    srcs = glob([
        "include/c++/**",
        "lib/clang/10.0.0/include/**",
    ]),
)

filegroup(
    name = "objcopy",
    srcs = ["bin/llvm-objcopy"],
)

filegroup(
    name = "strip",
    srcs = ["bin/llvm-strip"],
)
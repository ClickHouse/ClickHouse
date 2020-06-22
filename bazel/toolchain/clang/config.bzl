load("@bazel_tools//tools/build_defs/cc:action_names.bzl", "ACTION_NAMES")
load(
    "@bazel_tools//tools/cpp:cc_toolchain_config_lib.bzl",
    "feature",
    "flag_group",
    "flag_set",
    "tool_path",
)

def _impl(ctx):
    tool_paths = [
        tool_path(
            name = "ar",
            path = "bin/llvm-ar",
        ),
        tool_path(
            name = "cpp",
            path = "bin/clang",
        ),
        tool_path(
            name = "gcc",
            path = "bin/clang",
        ),
        tool_path(
            name = "gcov",
            path = "/usr/bin/false",
        ),
        tool_path(
            name = "ld",
            path = "bin/ld.lld",
        ),
        tool_path(
            name = "nm",
            path = "/usr/bin/false",
        ),
        tool_path(
            name = "objdump",
            path = "/usr/bin/false",
        ),
        tool_path(
            name = "strip",
            path = "/usr/bin/false",
        ),
    ]

    all_compile_actions = [
        ACTION_NAMES.c_compile,
        ACTION_NAMES.cpp_compile,
    ]

    default_compile_flags_feature = feature(
        name = "default_compile_flags",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = all_compile_actions,
                flag_groups = [
                    flag_group(
                        flags = [
                            "-no-canonical-prefixes",
                            "-Wall",
                            "-Werror",
                        ],
                    ),
                ],
            ),
        ],
    )

    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        features = [
            default_compile_flags_feature,
        ],
        toolchain_identifier = "clang-amd64-config",
        host_system_name = "unknown",
        target_system_name = "unknown",
        target_cpu = "k8",
        target_libc = "glibc-2.4",
        compiler = "clang",  # matches command-line option `--compiler`
        abi_version = "clang-11.0.0",
        abi_libc_version = "glibc-2.4",

        tool_paths = tool_paths,
        cxx_builtin_include_directories = [
            "/usr/include",  # TODO: replace with sysroot
        ],
    )

cc_toolchain_config = rule(
    attrs = {},
    implementation = _impl,
    provides = [CcToolchainConfigInfo],
)

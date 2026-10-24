"""Emit actual Bazel metadata, or reference CMake metadata for binary comparison."""

load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")

def _bazel_substitutions(ctx):
    toolchain = find_cc_toolchain(ctx)
    features = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    substitutions = {
        "@BAZEL_COMPILER_VERSION_DEFINES@": """// Generated configuration; compiler metadata is filled by Bazel.
#define CLICKHOUSE_BAZEL_STRINGIFY_IMPL(x) #x
#define CLICKHOUSE_BAZEL_STRINGIFY(x) CLICKHOUSE_BAZEL_STRINGIFY_IMPL(x)
#define CLICKHOUSE_BAZEL_COMPILER_VERSION CLICKHOUSE_BAZEL_STRINGIFY(__clang_major__) "." CLICKHOUSE_BAZEL_STRINGIFY(__clang_minor__) "." CLICKHOUSE_BAZEL_STRINGIFY(__clang_patchlevel__)
""",
        "@BAZEL_BUILD_SYSTEM_ENTRY@": '    "BUILD_SYSTEM", "Bazel",\n',
        "@BAZEL_CMAKE_VERSION@": json.encode(""),
        "@BAZEL_C_COMPILER_VERSION@": "CLICKHOUSE_BAZEL_COMPILER_VERSION",
        "@BAZEL_CXX_COMPILER_VERSION@": "CLICKHOUSE_BAZEL_COMPILER_VERSION",
    }
    for language, action, flags in [
        ("C", ACTION_NAMES.c_compile, ctx.fragments.cpp.conlyopts + ctx.attr.c_flags),
        ("CXX", ACTION_NAMES.cpp_compile, ctx.fragments.cpp.cxxopts + ctx.attr.cxx_flags),
    ]:
        variables = cc_common.create_compile_variables(
            feature_configuration = features,
            cc_toolchain = toolchain,
            user_compile_flags = ctx.fragments.cpp.copts + flags,
        )
        command = cc_common.get_memory_inefficient_command_line(
            feature_configuration = features,
            action_name = action,
            variables = variables,
        )
        substitutions["@BAZEL_" + language + "_COMPILER@"] = json.encode(cc_common.get_tool_for_action(
            feature_configuration = features,
            action_name = action,
        ))
        substitutions["@BAZEL_" + language + "_FLAGS@"] = json.encode(" ".join(command))
    variables = cc_common.create_link_variables(
        feature_configuration = features,
        cc_toolchain = toolchain,
        user_link_flags = ctx.attr.link_flags + ctx.fragments.cpp.linkopts,
        is_using_linker = True,
        is_linking_dynamic_library = False,
        is_static_linking_mode = True,
    )
    command = cc_common.get_memory_inefficient_command_line(
        feature_configuration = features,
        action_name = ACTION_NAMES.cpp_link_executable,
        variables = variables,
    )
    substitutions["@BAZEL_LINK_FLAGS@"] = json.encode(" ".join(command))
    return substitutions

def _build_options_impl(ctx):
    ctx.actions.expand_template(
        template = ctx.file.template,
        output = ctx.outputs.out,
        substitutions = ctx.attr.cmake_substitutions if ctx.attr.cmake_metadata else _bazel_substitutions(ctx),
    )
    return [DefaultInfo(files = depset([ctx.outputs.out]))]

build_options = rule(
    implementation = _build_options_impl,
    attrs = {
        "template": attr.label(allow_single_file = True, mandatory = True),
        "out": attr.output(mandatory = True),
        "c_flags": attr.string_list(),
        "cxx_flags": attr.string_list(),
        "link_flags": attr.string_list(),
        "cmake_metadata": attr.bool(),
        "cmake_substitutions": attr.string_dict(mandatory = True),
    },
    fragments = ["cpp"],
    toolchains = use_cc_toolchain(),
)

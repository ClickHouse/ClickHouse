"""Make the resolved Bazel C++ toolchain available to the CMake reference build."""

load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")

def _cmake_toolchain_impl(ctx):
    toolchain = find_cc_toolchain(ctx)
    features = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    manifest = ctx.actions.declare_file(ctx.label.name + ".json")
    paths = {
        name: cc_common.get_tool_for_action(
            feature_configuration = features,
            action_name = action,
        )
        for name, action in [
            ("cc", ACTION_NAMES.c_compile),
            ("cxx", ACTION_NAMES.cpp_compile),
            ("ar", ACTION_NAMES.cpp_link_static_library),
            ("link", ACTION_NAMES.cpp_link_executable),
        ]
    }
    paths["nasm"] = ctx.executable._nasm.path
    paths["python"] = ctx.file._python.path
    ctx.actions.write(manifest, json.encode_indent(paths) + "\n")
    return [DefaultInfo(files = depset(
        [manifest, ctx.file._python],
        transitive = [
            toolchain.all_files,
            ctx.attr._nasm[DefaultInfo].files,
            ctx.attr._python_files[DefaultInfo].files,
        ],
    ))]

cmake_toolchain = rule(
    implementation = _cmake_toolchain_impl,
    attrs = {
        "_nasm": attr.label(default = "@nasm//:nasm", executable = True, cfg = "exec"),
        "_python": attr.label(default = "@python_3_11//:python3", allow_single_file = True, cfg = "exec"),
        "_python_files": attr.label(default = "@python_3_11//:files", cfg = "exec"),
    },
    fragments = ["cpp"],
    toolchains = use_cc_toolchain(),
)

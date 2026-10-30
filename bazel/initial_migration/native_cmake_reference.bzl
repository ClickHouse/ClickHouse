"""Run the original ClickHouse CMake build with native ARM64 tools on RBE."""

load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")

def _native_cmake_reference_impl(ctx):
    cc = find_cc_toolchain(ctx)
    rust = ctx.toolchains["@rules_rust//rust:toolchain_type"]
    if rust.target_flag_value != "aarch64-unknown-linux-gnu":
        fail("Native ARM64 CMake requires the aarch64 Rust toolchain, got " + rust.target_flag_value)
    features = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    output = ctx.actions.declare_directory(ctx.label.name)
    args = ctx.actions.args()
    args.set_param_file_format("multiline")
    args.use_param_file("@%s", use_always = True)
    args.add("--source-archive", ctx.file.source_archive)
    args.add("--tools-archive", ctx.file.tools_archive)
    args.add("--cc", cc_common.get_tool_for_action(
        feature_configuration = features,
        action_name = ACTION_NAMES.c_compile,
    ))
    args.add("--cxx", cc_common.get_tool_for_action(
        feature_configuration = features,
        action_name = ACTION_NAMES.cpp_compile,
    ))
    args.add("--rustc", rust.rustc)
    args.add("--cargo", rust.cargo)
    args.add("--rust-sysroot-anchor", rust.sysroot_anchor)
    args.add("--cmake", ctx.file._cmake)
    args.add("--python", ctx.file._python)
    args.add("--output-dir", output.path)
    args.add("--jobs", ctx.attr.jobs)
    if ctx.attr.configure_only:
        args.add("--configure-only")
    ctx.actions.run(
        executable = ctx.attr._driver[DefaultInfo].files_to_run,
        arguments = [args],
        inputs = depset(
            [ctx.file.source_archive, ctx.file.tools_archive, rust.sysroot_anchor, ctx.file._cmake, ctx.file._python],
            transitive = [
                cc.all_files,
                rust.all_files,
                ctx.attr._cmake_files[DefaultInfo].files,
                ctx.attr._python_files[DefaultInfo].files,
            ],
        ),
        outputs = [output],
        env = rust.env,
        mnemonic = "NativeARM64CMake",
        progress_message = "Building ClickHouse with native ARM64 CMake and Ninja",
        execution_requirements = {"no-local": "1"},
    )
    return [DefaultInfo(files = depset([output]))]

native_cmake_reference = rule(
    implementation = _native_cmake_reference_impl,
    attrs = {
        "source_archive": attr.label(mandatory = True, allow_single_file = True),
        "tools_archive": attr.label(mandatory = True, allow_single_file = True),
        "configure_only": attr.bool(),
        "jobs": attr.int(default = 12),
        "_cmake": attr.label(default = "@clickhouse_cmake_linux_aarch64//:bin/cmake", allow_single_file = True, cfg = "exec"),
        "_cmake_files": attr.label(default = "@clickhouse_cmake_linux_aarch64//:files", cfg = "exec"),
        "_python": attr.label(default = "@python_3_11//:python3", allow_single_file = True, cfg = "exec"),
        "_python_files": attr.label(default = "@python_3_11//:files", cfg = "exec"),
        "_driver": attr.label(default = "//bazel/initial_migration:native_cmake_reference_driver", executable = True, cfg = "exec"),
    },
    fragments = ["cpp"],
    toolchains = use_cc_toolchain() + ["@rules_rust//rust:toolchain_type"],
)

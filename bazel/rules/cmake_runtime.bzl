"""Compile ClickHouse's runtime sources with their CMake flags and Bazel's Clang."""

load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@bazel_skylib//rules:common_settings.bzl", "BuildSettingInfo")

def _resource_dir(ctx, toolchain, features, action):
    # LLVM's registered toolchain declares its compile resource directory in
    # cc_args. Use that selected directory without importing its libc++ headers.
    variables = cc_common.create_compile_variables(
        feature_configuration = features,
        cc_toolchain = toolchain,
    )
    command = cc_common.get_memory_inefficient_command_line(
        feature_configuration = features,
        action_name = action,
        variables = variables,
    )
    directories = [arg for arg in command if arg.startswith("-resource-dir=")]
    if len(directories) != 1:
        fail("{}: expected one Clang compile resource directory, found {}".format(ctx.label, directories))
    return directories[0]

def _cmake_runtime_objects_impl(ctx):
    # One architecture may have fewer File API compile groups than the other.
    if not ctx.files.srcs:
        return [DefaultInfo(files = depset())]

    toolchain = find_cc_toolchain(ctx)
    features = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )

    # CMake uses Clang's C driver for preprocessed .S sources, too.
    action = ACTION_NAMES.cpp_compile if ctx.attr.language == "CXX" else ACTION_NAMES.c_compile
    compiler = cc_common.get_tool_for_action(
        feature_configuration = features,
        action_name = action,
    )
    triple = ctx.attr.arch + "-linux-gnu"
    gcc_root = ctx.file._sysroot_marker.dirname + "/linux-" + ctx.attr.arch
    header_inputs = depset(
        ctx.files.hdrs + [ctx.file._sysroot_marker],
        transitive = [toolchain.all_files, ctx.attr._sysroot[DefaultInfo].files],
    )
    labels = {target.label: target for target in ctx.attr.srcs + ctx.attr.hdrs}.values()
    copts = [
        ctx.expand_make_variables("copts", ctx.expand_location(option, targets = labels), {})
        for option in ctx.attr.copts
    ]
    flags = [
        "--target=" + triple,
        "--sysroot=" + gcc_root + "/" + triple + "/libc",
        "--gcc-toolchain=" + gcc_root,
        _resource_dir(ctx, toolchain, features, action),
    ] + ["-D" + define for define in ctx.attr.local_defines] + copts
    if ctx.attr._cmake_metadata[BuildSettingInfo].value and ctx.attr.reference_source_root:
        # These CMake groups lack -ffile-prefix-map. Their absolute __FILE__
        # strings are part of the executable and must use the recorded root.
        flags.append(
            "-fmacro-prefix-map=" + ctx.label.workspace_root + "/=" +
            ctx.attr.reference_source_root + "/contrib/llvm-project/",
        )
        # Retain the old Bazel filenames for generated inputs in runtime groups
        # which did not use CMake source-prefix mapping.
        generated = ctx.bin_dir.path + "/" + ctx.label.workspace_root + "/gen/"
        flags.append("-ffile-prefix-map=" + generated + "=" + generated + ctx.attr.arch + "/")

    outputs = []
    for src in ctx.files.srcs:
        # .pic.o makes cc_library retain these CMake PIE/PIC objects for a
        # shared or PIE link. CMake's flags, rather than this suffix, select PIC.
        output = ctx.actions.declare_file(ctx.label.name + "/" + src.path + ".pic.o")
        args = ctx.actions.args()
        args.add_all(flags)
        args.add_all(["-o", output.path, "-c", src.path])
        ctx.actions.run(
            executable = compiler,
            inputs = depset([src], transitive = [header_inputs]),
            outputs = [output],
            arguments = [args],
            mnemonic = "Assemble" if ctx.attr.language == "ASM" else "CppCompile",
            progress_message = "Compiling ClickHouse runtime %{input}",
        )
        outputs.append(output)
    return [DefaultInfo(files = depset(outputs))]

cmake_runtime_objects = rule(
    implementation = _cmake_runtime_objects_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "hdrs": attr.label_list(allow_files = True),
        "copts": attr.string_list(),
        "local_defines": attr.string_list(),
        "language": attr.string(values = ["C", "CXX", "ASM"], mandatory = True),
        "arch": attr.string(values = ["x86_64", "aarch64"], mandatory = True),
        "reference_source_root": attr.string(),
        "_cmake_metadata": attr.label(default = "@clickhouse//bazel:cmake_metadata"),
        "_sysroot": attr.label(default = "@clickhouse_sysroot//:linux_compile_files"),
        "_sysroot_marker": attr.label(default = "@clickhouse_sysroot//:BUILD.bazel", allow_single_file = True),
    },
    fragments = ["cpp"],
    toolchains = use_cc_toolchain(),
)

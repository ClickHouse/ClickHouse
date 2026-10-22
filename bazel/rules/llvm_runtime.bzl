"""Compile ClickHouse's runtime sources with their CMake flags and Bazel's Clang."""

load("@bazel_skylib//lib:paths.bzl", "paths")
load("@bazel_skylib//rules:common_settings.bzl", "BuildSettingInfo")
load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")

LlvmRuntimeCompileInfo = provider(
    "The original compile commands and inputs of configured LLVM runtime C/C++ sources.",
    fields = {
        "units": "Ordered structs(src, output, compiler, command, inputs), one per compile action.",
        "toolchain_files": "Files from the compiler toolchain; excludes other declared inputs.",
        "declared_inputs": "Private compiler input availability from hdrs, not header ownership or exports.",
        "language": "The configured C or CXX language.",
    },
)

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

def _llvm_runtime_objects_impl(ctx):
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
    gcc_root = ctx.attr._sysroot.label.workspace_root + "/linux-" + ctx.attr.arch
    header_inputs = depset(
        ctx.files.hdrs,
        transitive = [toolchain.all_files, ctx.attr._sysroot[DefaultInfo].files],
    )
    labels = {target.label: target for target in ctx.attr.srcs + ctx.attr.hdrs}.values()
    copts = [
        ctx.expand_make_variables("copts", ctx.expand_location(option, targets = labels), {})
        for option in ctx.attr.copts
    ]
    include_flags = []
    for directory in ctx.attr.local_includes:
        include_dir = paths.join(ctx.label.workspace_root, ctx.label.package, directory)
        include_flags.extend(["-I", include_dir, "-I", paths.join(ctx.bin_dir.path, include_dir)])
    system_flags = []
    for directory in ctx.attr.system_includes:
        include_dir = paths.join(ctx.label.workspace_root, ctx.label.package, directory)
        system_flags.extend(["-isystem", include_dir])
    flags = [
        "--target=" + triple,
        "--sysroot=" + gcc_root + "/" + triple + "/libc",
        "--gcc-toolchain=" + gcc_root,
        _resource_dir(ctx, toolchain, features, action),
    ] + ["-D" + define for define in ctx.attr.local_defines] + include_flags + copts + system_flags
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
    units = []
    extensions = {
        "C": ["c"],
        "CXX": ["cc", "cpp", "cxx", "c++", "C"],
        "ASM": [],
    }[ctx.attr.language]
    collect_units = all([not src.is_directory and src.extension in extensions for src in ctx.files.srcs])
    for src in ctx.files.srcs:
        # .pic.o makes cc_library retain these CMake PIE/PIC objects for a
        # shared or PIE link. CMake's flags, rather than this suffix, select PIC.
        output = ctx.actions.declare_file(ctx.label.name + "/" + src.path + ".pic.o")
        command = flags + ["-o", output.path, "-c", src.path]
        inputs = depset([src], transitive = [header_inputs])
        args = ctx.actions.args()
        args.add_all(command)
        ctx.actions.run(
            executable = compiler,
            inputs = inputs,
            outputs = [output],
            arguments = [args],
            mnemonic = "Assemble" if ctx.attr.language == "ASM" else "CppCompile",
            progress_message = "Compiling ClickHouse runtime %{input}",
        )
        outputs.append(output)
        if collect_units:
            units.append(struct(src = src, output = output, compiler = compiler, command = command, inputs = inputs))
    result = [DefaultInfo(files = depset(outputs))]
    if collect_units:
        result.append(LlvmRuntimeCompileInfo(
            units = units,
            toolchain_files = toolchain.all_files,
            declared_inputs = depset(ctx.files.hdrs),
            language = ctx.attr.language,
        ))
    return result

llvm_runtime_objects = rule(
    implementation = _llvm_runtime_objects_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "hdrs": attr.label_list(allow_files = True),
        "copts": attr.string_list(),
        "local_defines": attr.string_list(),
        "local_includes": attr.string_list(doc = "Private package-relative source and generated include directories."),
        "system_includes": attr.string_list(doc = "Private package-relative LLVM source directories searched as system headers."),
        "language": attr.string(values = ["C", "CXX", "ASM"], mandatory = True),
        "arch": attr.string(values = ["x86_64", "aarch64"], mandatory = True),
        "reference_source_root": attr.string(),
        "_cmake_metadata": attr.label(default = "@clickhouse//bazel:cmake_metadata"),
        "_sysroot": attr.label(default = "@clickhouse_sysroot//:linux_compile_files"),
    },
    fragments = ["cpp"],
    toolchains = use_cc_toolchain(),
)

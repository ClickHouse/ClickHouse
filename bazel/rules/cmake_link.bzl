"""Link Bazel compilation outputs in the order recorded by CMake."""

load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")
load("//bazel/rules:cmake_link_data.bzl", "CMAKE_LINK")
load("//bazel/rules:cmake_link_data_aarch64.bzl", CMAKE_LINK_AARCH64 = "CMAKE_LINK")

_CMAKE_LINKS = {
    "x86_64": CMAKE_LINK,
    "aarch64": CMAKE_LINK_AARCH64,
}

def _archive(library):
    return library.pic_static_library or library.static_library

def _own_library(target):
    candidates = []
    linking_inputs = target[CcInfo].linking_context.linker_inputs.to_list()
    for linker_input in linking_inputs:
        if linker_input.owner == target.label:
            for library in linker_input.libraries:
                if _archive(library):
                    candidates.append(library)
    if not candidates:
        # An alias forwards the actual target's CcInfo and DefaultInfo. Accept
        # only archives explicitly exported by DefaultInfo, never an arbitrary
        # transitive library. Root cc_imports normally take the owner branch.
        exported = {file: True for file in target[DefaultInfo].files.to_list()}
        for linker_input in linking_inputs:
            for library in linker_input.libraries:
                if _archive(library) in exported:
                    candidates.append(library)
    if len(candidates) != 1:
        fail("{}: expected exactly one owned archive, got {}".format(
            target.label,
            [_archive(library).path for library in candidates],
        ))
    return candidates[0]

def _objects(ranges, libraries):
    files = []
    for ref, start, end in ranges:
        library = libraries[ref]
        objects = library.pic_objects or library.objects
        if objects == None or start < 0 or start >= end or end > len(objects):
            fail("Missing Bazel object range [{}, {}) in {}".format(start, end, _archive(library)))
        files.extend(objects[start:end])
    return files

def _startup_directory(objects, names):
    if sorted([obj.basename for obj in objects]) != names or len({obj.dirname: True for obj in objects}) != 1:
        fail("Expected {} in one startup directory, got {}".format(names, objects))
    return objects[0].dirname

def _cmake_link_impl(ctx):
    plan = _CMAKE_LINKS[ctx.attr.arch]
    if len(ctx.attr.refs) != len(plan["expected_objects"]):
        fail("CMake {} link plan and refs do not describe the same link".format(ctx.attr.arch))
    libraries = []
    for i, target in enumerate(ctx.attr.refs):
        library = _own_library(target)
        expected = plan["expected_objects"][i]
        if expected >= 0:
            objects = library.pic_objects or library.objects
            if objects == None or len(objects) != expected:
                fail("{}: CMake requires {} objects; Bazel provides {}".format(
                    target.label,
                    expected,
                    len(objects) if objects != None else "no objects",
                ))
        libraries.append(library)

    toolchain = find_cc_toolchain(ctx)
    features = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    clang = cc_common.get_tool_for_action(
        feature_configuration = features,
        action_name = ACTION_NAMES.cpp_link_executable,
    )
    ar = cc_common.get_tool_for_action(
        feature_configuration = features,
        action_name = ACTION_NAMES.cpp_link_static_library,
    )
    archives = []
    for record in plan["archives"]:
        if "objects" in record:
            files = _objects(record["objects"], libraries)
            output = ctx.actions.declare_file(ctx.label.name + "_archives/" + record["name"])
            args = ctx.actions.args()
            args.use_param_file("@%s", use_always = True)
            args.set_param_file_format("multiline")
            args.add("rcsD")
            args.add(output)
            args.add_all(files)
            ctx.actions.run(
                executable = ar,
                inputs = depset(files, transitive = [toolchain.all_files]),
                arguments = [args],
                outputs = [output],
                mnemonic = "ClickHouseNativeArchive",
                progress_message = "Archiving Bazel objects for " + record["name"],
            )
            archives.append(output)
        else:
            original = _archive(libraries[record["file"]])
            if "append" in record and not ctx.attr.cargo_archives:
                # The default rules_rs build compiles Skim's cxx sources
                # separately. The native Cargo build already bundles them.
                files = _objects(record["append"], libraries)
                output = ctx.actions.declare_file(ctx.label.name + "_archives/" + record["name"])
                script = ctx.actions.declare_file(ctx.label.name + "_archives/" + record["name"] + ".mri")
                ctx.actions.write(
                    script,
                    "CREATE " + output.path + "\nADDLIB " + original.path + "\n" +
                    "".join(["ADDMOD " + f.path + "\n" for f in files]) + "SAVE\nEND\n",
                )
                ctx.actions.run_shell(
                    inputs = depset([script, original] + files, transitive = [toolchain.all_files]),
                    arguments = [ar, script.path],
                    outputs = [output],
                    command = '"$1" -M < "$2"',
                    mnemonic = "ClickHouseRustArchive",
                    progress_message = "Bundling Bazel Rust and cxx objects for " + record["name"],
                )
                archives.append(output)
            else:
                archives.append(original)

    output = ctx.actions.declare_file(ctx.label.name)

    # Only relative Bazel artifact names are used for filesystem access.
    sysroot = "{}/linux-{}/{}-linux-gnu/libc".format(ctx.attr._sysroot.label.workspace_root, ctx.attr.arch, ctx.attr.arch)
    args = ctx.actions.args()
    args.use_param_file("@%s", use_always = True)
    args.set_param_file_format("multiline")
    startup_dirs = [
        _startup_directory(ctx.files._glibc_startup, ["Scrt1.o", "crti.o", "crtn.o"]),
        _startup_directory(ctx.files._gcc_startup, ["crtbeginS.o", "crtendS.o"]),
    ]
    inputs = ctx.files._glibc_startup + ctx.files._gcc_startup
    for operand in plan["sequence"]:
        kind = operand[0]
        if kind == "archive":
            artifact = archives[operand[1]]
            inputs.append(artifact)
            args.add(artifact)
        elif kind == "objects":
            for artifact in _objects(operand[1], libraries):
                inputs.append(artifact)
                args.add(artifact)
        elif kind == "arg":
            args.add(operand[1])
        elif kind == "gcc":
            args.add_all(startup_dirs, format_each = "-B%s/")
        elif kind == "sysroot":
            args.add("--sysroot=" + sysroot)
        elif kind == "lld":
            args.add("--ld-path=" + ctx.executable._lld.path)
        else:
            fail("Unknown CMake operand type: {}".format(operand))
    args.add("-o")
    args.add(output)
    ctx.actions.run(
        executable = clang,
        inputs = depset(inputs, transitive = [
            toolchain.all_files,
            ctx.attr._sysroot[DefaultInfo].files,
        ]),
        tools = [ctx.attr._lld[DefaultInfo].files_to_run],
        outputs = [output],
        arguments = [args],
        mnemonic = "ClickHouseNativeLink",
        progress_message = "Linking %{label} in CMake order from Bazel artifacts",
    )
    return [DefaultInfo(files = depset([output]), executable = output)]

_cmake_link = rule(
    implementation = _cmake_link_impl,
    executable = True,
    attrs = {
        "arch": attr.string(values = ["x86_64", "aarch64"], mandatory = True),
        "cargo_archives": attr.bool(mandatory = True),
        "refs": attr.label_list(mandatory = True, providers = [CcInfo]),
        "_glibc_startup": attr.label(default = "@llvm//runtimes/glibc:pie_startup_objects"),
        "_gcc_startup": attr.label(default = "@clickhouse_sysroot//:gcc_pie_startup_objects"),
        "_sysroot": attr.label(default = "@clickhouse_sysroot//:linux_link_files"),
        "_lld": attr.label(default = "@llvm//tools:ld.lld", executable = True, cfg = "exec"),
    },
    fragments = ["cpp"],
    toolchains = use_cc_toolchain(),
)

def cmake_link(name, **kwargs):
    """Link the Bazel-built ClickHouse objects with its captured CMake operands."""
    _cmake_link(
        name = name,
        arch = select({
            "@platforms//cpu:" + arch: arch
            for arch in _CMAKE_LINKS
        }),
        cargo_archives = select({
            "//bazel:use_cmake_metadata": True,
            "//conditions:default": False,
        }),
        refs = select({
            "@platforms//cpu:" + arch: plan["refs"]
            for arch, plan in _CMAKE_LINKS.items()
        }),
        **kwargs
    )

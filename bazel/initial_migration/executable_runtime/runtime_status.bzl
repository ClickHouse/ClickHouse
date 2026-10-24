"""Native-executor evidence for plain C++ consumers of ClickHouse's zlib."""

load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")

def _files(files):
    return [file.path for file in files or []]

def _link_inputs(target):
    result = []
    for linker_input in target[CcInfo].linking_context.linker_inputs.to_list():
        libraries = []
        for lib in linker_input.libraries:
            libraries.append({
                "static": lib.static_library.path if lib.static_library else None,
                "pic_static": lib.pic_static_library.path if lib.pic_static_library else None,
                "dynamic": lib.dynamic_library.path if lib.dynamic_library else None,
                "interface": lib.interface_library.path if lib.interface_library else None,
                "objects": _files(lib.objects),
                "pic_objects": _files(lib.pic_objects),
                "alwayslink": lib.alwayslink,
            })
        result.append({
            "owner": str(linker_input.owner),
            "libraries": libraries,
            "flags": linker_input.user_link_flags,
            "additional_inputs": _files(linker_input.additional_inputs),
        })
    return result

def _outputs(target):
    return [file.path for file in target[DefaultInfo].files.to_list()]

def _runtime_status_impl(ctx):
    binary = ctx.attr.binary[DefaultInfo].files_to_run
    test = ctx.attr.test[DefaultInfo].files_to_run
    if binary.executable == None or test.executable == None:
        fail("public_zlib_binary and public_zlib_test must provide executables")
    manifest = ctx.actions.declare_file(ctx.label.name + ".configured.json")
    status = ctx.actions.declare_file(ctx.label.name + ".status.json")
    ctx.actions.write(manifest, json.encode_indent({
        "arch": ctx.attr.arch,
        "required_runtime": _link_inputs(ctx.attr._required_runtime),
        "exec_required_runtime": _link_inputs(ctx.attr._exec_required_runtime),
        "target_runtime": _link_inputs(ctx.attr._target_runtime),
        "exec_runtime": _link_inputs(ctx.attr._exec_runtime),
        "zlib_library": _link_inputs(ctx.attr._zlib),
        "outputs": {
            "binary": _outputs(ctx.attr.binary),
            "test": _outputs(ctx.attr.test),
            "shared": _outputs(ctx.attr.shared),
            "explicit_empty": _outputs(ctx.attr.explicit_empty),
            "exec_binary": _outputs(ctx.attr._exec_binary),
        },
    }) + "\n")
    args = ctx.actions.args()
    args.add("--configured", manifest)
    args.add("--binary", binary.executable)
    args.add("--test", test.executable)
    args.add("--status", status)
    ctx.actions.run(
        executable = ctx.attr._driver[DefaultInfo].files_to_run,
        arguments = [args],
        inputs = [manifest],
        tools = [binary, test],
        outputs = [status],
        mnemonic = "ClickHousePublicZlibRuntime",
        progress_message = "Checking public zlib links and XRay ELF preinit on %{label}",
        execution_requirements = {"no-local": "1"},
    )

    # The configured manifest describes actual CcInfo and the exact configured
    # binaries. The subsequent same-invocation aquery checks their CppLink argv.
    return [DefaultInfo(files = depset([manifest, status]))]

runtime_status = rule(
    implementation = _runtime_status_impl,
    attrs = {
        "arch": attr.string(mandatory = True, values = ["x86_64", "aarch64"]),
        "binary": attr.label(mandatory = True),
        "test": attr.label(mandatory = True),
        "shared": attr.label(mandatory = True),
        "explicit_empty": attr.label(mandatory = True),
        "_required_runtime": attr.label(
            default = "@clickhouse_llvm_project//:clang_rt_builtins",
            providers = [CcInfo],
        ),
        "_target_runtime": attr.label(
            default = "@bazel_tools//tools/cpp:link_extra_libs",
            providers = [CcInfo],
        ),
        "_exec_required_runtime": attr.label(
            default = "@clickhouse_llvm_project//:clang_rt_builtins",
            providers = [CcInfo],
            cfg = "exec",
        ),
        "_exec_runtime": attr.label(
            default = "@bazel_tools//tools/cpp:link_extra_libs",
            providers = [CcInfo],
            cfg = "exec",
        ),
        "_zlib": attr.label(
            default = "@clickhouse_zlib_ng//:_zlib",
            providers = [CcInfo],
        ),
        "_exec_binary": attr.label(default = ":public_zlib_binary", cfg = "exec"),
        "_driver": attr.label(
            default = ":runtime_status_driver",
            executable = True,
            cfg = "exec",
        ),
    },
)

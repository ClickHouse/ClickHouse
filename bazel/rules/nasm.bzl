"""NASM assembly using the executable built from its BCR module."""

load("@bazel_skylib//lib:paths.bzl", "paths")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")

def _nasm_objects_impl(ctx):
    outputs = []
    flags = [ctx.expand_make_variables("copts", flag, {}) for flag in ctx.attr.copts]
    flags += ["-D" + define for define in ctx.attr.local_defines]
    package = paths.join(ctx.label.workspace_root, ctx.label.package)
    for directory in ctx.attr.local_includes:
        include = paths.join(package, directory)
        flags += ["-I", include + "/", "-I", paths.join(ctx.bin_dir.path, include) + "/"]
    headers = cc_common.merge_compilation_contexts(
        compilation_contexts = [dep[CcInfo].compilation_context for dep in ctx.attr.deps],
    )
    for directories in [headers.quote_includes, headers.includes, headers.system_includes, headers.external_includes]:
        for directory in directories.to_list():
            flags += ["-I", directory + "/"]
    for src in ctx.files.srcs:
        output = ctx.actions.declare_file(ctx.label.name + "/" + src.short_path + ".o")
        args = ctx.actions.args()
        args.add_all(flags)
        args.add_all(["-f", "elf64", "-o", output.path, src.path])
        ctx.actions.run(
            executable = ctx.executable._nasm,
            arguments = [args],
            inputs = depset([src] + ctx.files.hdrs, transitive = [headers.headers]),
            outputs = [output],
            mnemonic = "NasmCompile",
            progress_message = "Assembling %{input}",
        )
        outputs.append(output)
    return [DefaultInfo(files = depset(outputs))]

nasm_objects = rule(
    implementation = _nasm_objects_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = [".asm"]),
        "hdrs": attr.label_list(allow_files = True),
        "copts": attr.string_list(),
        "local_defines": attr.string_list(),
        "local_includes": attr.string_list(doc = "NASM include directories relative to this target's package."),
        "deps": attr.label_list(providers = [CcInfo]),
        "_nasm": attr.label(default = "@nasm//:nasm", executable = True, cfg = "exec"),
    },
)

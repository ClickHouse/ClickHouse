"""Preserve ClickHouse's Rust symbol boundaries before linking into C++."""

load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")

def _rust_archive_impl(ctx):
    references = []
    for target in ctx.attr.references:
        archives = []
        for linker_input in target[CcInfo].linking_context.linker_inputs.to_list():
            # Localize against each runtime's own archive, not its dependencies.
            if linker_input.owner != target.label:
                continue
            for library in linker_input.libraries:
                archive = library.static_library or library.pic_static_library
                if archive:
                    archives.append(archive)
        if not archives:
            fail("Rust localization requires a static archive from " + str(target.label))
        references.extend(archives)
    references.append(ctx.file._libm_abi)
    output = ctx.actions.declare_file("lib" + ctx.label.name + ".a")
    args = ctx.actions.args()
    args.add("--source", ctx.file.src)
    args.add("--output", output)
    tools = []
    for name in ["ar", "objcopy", "nm", "ld"]:
        args.add("--" + name, getattr(ctx.executable, "_" + name))
        tools.append(getattr(ctx.executable, "_" + name))
    args.add("--localize-script", ctx.file._localize_script)
    args.add("--strip-script", ctx.file._strip_script)
    args.add_all(references, before_each = "--reference")
    args.add_all(ctx.attr.keep_symbols, before_each = "--keep")
    ctx.actions.run(
        executable = ctx.attr._processor[DefaultInfo].files_to_run,
        arguments = [args],
        inputs = depset([ctx.file.src, ctx.file._localize_script, ctx.file._strip_script] + references),
        tools = tools,
        outputs = [output],
        mnemonic = "ClickHouseRustSymbols",
        progress_message = "Processing Rust symbols in %{label}",
    )
    return [DefaultInfo(files = depset([output]))]

rust_archive = rule(
    implementation = _rust_archive_impl,
    attrs = {
        "src": attr.label(allow_single_file = [".a"]),
        "references": attr.label_list(providers = [CcInfo]),
        "keep_symbols": attr.string_list(),
        "_processor": attr.label(default = "//bazel/rules:process_rust_archive", executable = True, cfg = "exec"),
        "_localize_script": attr.label(default = "//:cmake/localize_rust_c_symbols.sh", allow_single_file = True),
        "_strip_script": attr.label(default = "//:cmake/strip_rust_symbols.sh", allow_single_file = True),
        "_libm_abi": attr.label(default = "@glibc//:libm_abi", allow_single_file = [".abilist"]),
    } | {
        "_" + name: attr.label(default = "@llvm//tools:" + tool, executable = True, allow_files = True, cfg = "exec")
        for name, tool in {"ar": "llvm-ar", "objcopy": "llvm-objcopy", "nm": "llvm-nm", "ld": "ld.lld"}.items()
    },
)

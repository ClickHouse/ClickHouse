"""Generate C and C++ headers from Cargo crates with the pinned Rust toolchain."""

load(":cargo_sources.bzl", "cargo_source_inputs")

def _rust_cbindgen_impl(ctx):
    rust = ctx.toolchains["@rules_rust//rust:toolchain_type"]
    rust_files = rust.all_files
    if ctx.attr.cargo_runtime:
        rust_files = ctx.attr.cargo_runtime[DefaultInfo].default_runfiles.files
    outputs = []
    args = ctx.actions.args()
    args.add("--cargo", rust.cargo)
    args.add("--rustc", rust.rustc)
    args.add("--cbindgen", ctx.executable.cbindgen_tool)
    args.add("--crate", ctx.attr.crate)
    args.add("--manifest", ctx.file.manifest)
    args.add("--config", ctx.file.config)
    args.add("--cargo-config", ctx.file.cargo_config)
    args.add("--vendor", ctx.attr.vendor.label.workspace_root)
    for language, filename in ctx.attr.headers.items():
        output = ctx.actions.declare_file(ctx.label.name + "/" + filename)
        outputs.append(output)
        args.add_all(["--header", language, output])
    ctx.actions.run(
        executable = ctx.attr._generator[DefaultInfo].files_to_run,
        arguments = [args],
        inputs = depset(
            [ctx.file.manifest, ctx.file.config, ctx.file.cargo_config],
            transitive = [rust_files, cargo_source_inputs(ctx.attr.vendor)] + [cargo_source_inputs(src) for src in ctx.attr.srcs],
        ),
        tools = [ctx.attr.cbindgen_tool[DefaultInfo].files_to_run],
        outputs = outputs,
        mnemonic = "RustCbindgen",
        progress_message = "Generating C/C++ bindings for %{label}",
    )
    return [DefaultInfo(files = depset(outputs))]

rust_cbindgen = rule(
    implementation = _rust_cbindgen_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "crate": attr.string(mandatory = True),
        "manifest": attr.label(allow_single_file = True, mandatory = True),
        "config": attr.label(allow_single_file = True, mandatory = True),
        "cargo_config": attr.label(allow_single_file = True, mandatory = True),
        "cargo_runtime": attr.label(doc = "Cargo runtime for configs with no parse.expand.crates. Omit for macro expansion with Cargo."),
        "vendor": attr.label(mandatory = True),
        "headers": attr.string_dict(mandatory = True, doc = "cbindgen language to header filename, relative to the target name."),
        "cbindgen_tool": attr.label(executable = True, cfg = "exec", mandatory = True),
        "_generator": attr.label(default = "//bazel/rules:cbindgen", executable = True, cfg = "exec"),
    },
    toolchains = ["@rules_rust//rust:toolchain_type"],
)

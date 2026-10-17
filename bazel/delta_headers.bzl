"""Declared offline cbindgen action using the rules_rs Rust toolchain."""

def _delta_headers_impl(ctx):
    rust = ctx.toolchains["@rules_rust//rust:toolchain_type"]
    outputs = [ctx.actions.declare_file(ctx.label.name + "/delta_kernel_ffi." + ext) for ext in ["h", "hpp"]]
    args = ctx.actions.args()
    args.add("--cargo", rust.cargo)
    args.add("--rustc", rust.rustc)
    args.add("--cbindgen", ctx.executable._cbindgen)
    args.add("--output", outputs[0].dirname)
    args.add("--manifest", ctx.file.manifest)
    args.add("--vendor", ctx.file.vendor.dirname)
    args.add("--config", ctx.file.config)
    ctx.actions.run(
        executable = ctx.attr._generator[DefaultInfo].files_to_run,
        arguments = [args],
        inputs = depset(ctx.files.srcs + [ctx.file.manifest, ctx.file.config, ctx.file.vendor], transitive = [rust.all_files]),
        tools = [ctx.attr._cbindgen[DefaultInfo].files_to_run],
        outputs = outputs,
        mnemonic = "DeltaKernelHeaders",
        progress_message = "Generating Delta Kernel C/C++ bindings",
    )
    return [DefaultInfo(files = depset(outputs))]

delta_headers = rule(
    implementation = _delta_headers_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "manifest": attr.label(default = "@clickhouse_delta_kernel_rs//:ffi/Cargo.toml", allow_single_file = True),
        "vendor": attr.label(default = "@clickhouse_rust_vendor//:BUILD.bazel", allow_single_file = True),
        "config": attr.label(default = "//:contrib/corrosion-cmake/config.toml.in", allow_single_file = True),
        "_generator": attr.label(default = "//bazel:generate_delta_headers", executable = True, cfg = "exec"),
        "_cbindgen": attr.label(default = "@delta_crates//:cbindgen_cli", executable = True, cfg = "exec"),
    },
    toolchains = ["@rules_rust//rust:toolchain_type"],
)

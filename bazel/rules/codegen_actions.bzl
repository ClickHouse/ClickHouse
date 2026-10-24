"""Declared license rendering and copies of source files read by Clang #embed."""

def _licenses_impl(ctx):
    if len(ctx.files.srcs) != len(ctx.attr.source_paths):
        fail("Each source_paths entry must have exactly one input file in srcs")
    inputs = ctx.files.srcs + [ctx.file.manifest]
    args = ctx.actions.args()
    args.add(ctx.file.manifest)
    args.add(ctx.outputs.out)
    for original, source in zip(ctx.attr.source_paths, ctx.files.srcs):
        args.add(original)
        args.add(source)
    if ctx.file.cmake_metadata:
        inputs.append(ctx.file.cmake_metadata)
        args.add("--cmake-metadata")
        args.add(ctx.file.cmake_metadata)
    args.use_param_file("@%s", use_always = True)
    args.set_param_file_format("multiline")
    ctx.actions.run(
        inputs = depset(inputs),
        outputs = [ctx.outputs.out],
        executable = ctx.executable._generator,
        arguments = [args],
        mnemonic = "ClickHouseLicenses",
    )
    return [DefaultInfo(files = depset([ctx.outputs.out]))]

_licenses = rule(
    implementation = _licenses_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "source_paths": attr.string_list(),
        "out": attr.output(mandatory = True),
        "manifest": attr.label(
            default = Label("//bazel/rules:license_manifest.json"),
            allow_single_file = True,
        ),
        "cmake_metadata": attr.label(allow_single_file = True),
        "_generator": attr.label(
            default = Label("//bazel/rules:generate_licenses"),
            executable = True,
            cfg = "exec",
        ),
    },
)

def _embedded_sources_impl(ctx):
    if len(ctx.files.srcs) != len(ctx.outputs.outs):
        fail("Each #embed output must have exactly one source file in srcs")
    for source, output in zip(ctx.files.srcs, ctx.outputs.outs):
        ctx.actions.run_shell(
            inputs = [source],
            outputs = [output],
            command = 'cp "$1" "$2"',
            arguments = [source.path, output.path],
            mnemonic = "ClickHouseEmbeddedSource",
        )
    return [DefaultInfo(files = depset(ctx.outputs.outs))]

_embedded_sources = rule(
    implementation = _embedded_sources_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "outs": attr.output_list(mandatory = True),
    },
)

def declare_codegen(name, attrs):
    """Instantiate one recorded generator with the metadata belonging to it."""
    kind = attrs.get("kind", "genrule")
    if kind == "licenses":
        _licenses(
            name = name,
            srcs = attrs["srcs"],
            out = attrs["outs"][0],
            source_paths = attrs["source_paths"],
            cmake_metadata = attrs.get("cmake_metadata"),
        )
    elif kind == "embedded_sources":
        _embedded_sources(
            name = name,
            srcs = attrs["srcs"],
            outs = attrs["outs"],
        )
    elif kind == "genrule":
        native.genrule(
            name = name,
            srcs = attrs["srcs"],
            outs = attrs["outs"],
            tools = attrs["tools"],
            cmd = attrs["cmd"],
        )
    else:
        fail("Unknown codegen kind for %s: %s" % (name, kind))

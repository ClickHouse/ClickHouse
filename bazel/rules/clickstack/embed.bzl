"""Compress the Next export for WebUIRequestHandler.cpp."""

def _embed_clickstack_impl(ctx):
    header = ctx.actions.declare_file("gen/contrib/clickstack-cmake/ClickStackResources.generated.h")
    packed = ctx.actions.declare_directory("gen/contrib/clickstack-cmake/clickstack_compressed")
    args = ctx.actions.args()
    args.add("--source", ctx.file.src.path)
    args.add("--output", header.path)
    args.add("--namespace", "ClickStack")
    args.add("--compressed", packed.basename)
    if ctx.attr.gzip_mtime >= 0:
        args.add("--cmake-gzip-mtime", ctx.attr.gzip_mtime)
    ctx.actions.run(
        executable = ctx.executable._generator,
        inputs = [ctx.file.src],
        outputs = [header, packed],
        arguments = [args],
        mnemonic = "ClickStackResources",
    )
    return [
        DefaultInfo(files = depset([header, packed])),
        OutputGroupInfo(header = depset([header]), assets = depset([packed])),
    ]

embed_clickstack = rule(
    implementation = _embed_clickstack_impl,
    attrs = {
        "src": attr.label(allow_single_file = True, mandatory = True),
        "gzip_mtime": attr.int(default = -1),
        "_generator": attr.label(
            default = "//bazel/rules:embed_resources",
            executable = True,
            cfg = "exec",
        ),
    },
)

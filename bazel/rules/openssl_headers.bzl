"""Assemble OpenSSL's source and configured headers for Cargo's directory API."""

def _openssl_headers_impl(ctx):
    output = ctx.actions.declare_directory(ctx.label.name)
    args = ctx.actions.args()
    args.add(output.path)
    for root, files in [
        (ctx.attr.source_headers.label.workspace_root + "/include/", ctx.files.source_headers),
        ("contrib/openssl-cmake/common/include/", ctx.files.configured_headers),
    ]:
        for header in files:
            if not header.path.startswith(root):
                fail("OpenSSL header outside its include root: " + header.path)
            args.add(header.path)
            args.add(header.path.removeprefix(root))
    ctx.actions.run_shell(
        inputs = ctx.files.source_headers + ctx.files.configured_headers,
        outputs = [output],
        arguments = [args],
        command = 'set -e; out="$1"; shift; while [ "$#" -gt 0 ]; do mkdir -p "$out/$(dirname "$2")"; cp -L "$1" "$out/$2"; shift 2; done',
        mnemonic = "OpenSSLHeaders",
    )
    return [DefaultInfo(files = depset([output]))]

openssl_headers = rule(
    implementation = _openssl_headers_impl,
    attrs = {
        "source_headers": attr.label(default = "@clickhouse_openssl//:headers"),
        "configured_headers": attr.label(mandatory = True),
    },
)

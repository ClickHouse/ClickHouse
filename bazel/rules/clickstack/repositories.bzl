"""Pinned Google Fonts and Yarn dependencies for the offline Next build."""

_NODE_ATTRS = {
    "node_darwin_amd64": attr.label(default = "@nodejs_darwin_amd64//:bin/node", allow_single_file = True),
    "node_darwin_arm64": attr.label(default = "@nodejs_darwin_arm64//:bin/node", allow_single_file = True),
    "node_linux_amd64": attr.label(default = "@nodejs_linux_amd64//:bin/node", allow_single_file = True),
    "node_linux_arm64": attr.label(default = "@nodejs_linux_arm64//:bin/node", allow_single_file = True),
}

def _host_node(ctx):
    system = ctx.os.name
    arm = ctx.os.arch in ("aarch64", "arm64")
    if system == "linux":
        return ctx.path(ctx.attr.node_linux_arm64 if arm else ctx.attr.node_linux_amd64)
    if system == "mac os x":
        return ctx.path(ctx.attr.node_darwin_arm64 if arm else ctx.attr.node_darwin_amd64)
    fail("HyperDX dependencies can be fetched on Linux and macOS: " + system)

def _clickstack_google_fonts_impl(ctx):
    manifest = ctx.read(ctx.attr.manifest)
    css = {ctx.path(label).basename: ctx.path(label) for label in ctx.attr.css}
    fetched = {}
    for entry in json.decode(manifest):
        family = entry["url"].split("family=")[1].split(":")[0].replace("+", "_")

        # Google mutates the CSS responses without changing their URLs. Keep
        # these four verified source responses, and fetch only versioned fonts.
        # build_hyperdx verifies both against this manifest in the build action.
        ctx.symlink(css.pop(family + ".css"), entry["css"])
        for font in entry["fonts"]:
            if font["file"] not in fetched:
                fetched[font["file"]] = ctx.download(
                    url = font["url"],
                    output = font["file"],
                    sha256 = font["sha256"],
                    block = False,
                )
    if css:
        fail("Unused Google CSS inputs: %s" % css.keys())
    for transfer in fetched.values():
        transfer.wait()
    ctx.file("manifest.json", manifest)
    ctx.file("BUILD.bazel", """
package(default_visibility = ["//visibility:public"])

filegroup(
    name = "manifest",
    srcs = ["manifest.json"],
)

filegroup(
    name = "fonts",
    srcs = glob(["css/*.css", "fonts/*.woff2"]),
)
""")

clickstack_google_fonts = repository_rule(
    implementation = _clickstack_google_fonts_impl,
    attrs = {
        "css": attr.label_list(mandatory = True, allow_files = [".css"]),
        "manifest": attr.label(mandatory = True, allow_single_file = True),
    },
)

def _clickstack_yarn_cache_impl(ctx):
    result = ctx.execute([
        str(_host_node(ctx)),
        str(ctx.path(ctx.attr.populate)),
        str(ctx.path(ctx.attr.package_json)),
        str(ctx.path(ctx.attr.native_checksums)),
        str(ctx.path(".")),
    ], timeout = 1200, quiet = False)
    if result.return_code:
        fail("HyperDX Yarn lockfile fetch failed: %s" % result.stderr)
    ctx.file("BUILD.bazel", """
package(default_visibility = ["//visibility:public"])

filegroup(
    name = "manifest",
    srcs = ["manifest.json"],
)

filegroup(
    name = "cache",
    srcs = glob(["cache/*.zip"]),
)
""")

clickstack_yarn_cache = repository_rule(
    implementation = _clickstack_yarn_cache_impl,
    attrs = dict(_NODE_ATTRS, **{
        "native_checksums": attr.label(mandatory = True, allow_single_file = True),
        "package_json": attr.label(default = "@clickhouse_hyperdx_src//:package.json", allow_single_file = True),
        "populate": attr.label(default = ":populate_yarn_cache.cjs", allow_single_file = True),
    }),
)

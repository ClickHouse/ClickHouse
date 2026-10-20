"""Next static export from HyperDX's own Yarn lockfile and verified offline inputs."""

load("@aspect_rules_js//js:defs.bzl", "js_run_binary")

_FONT_COVERAGE = {
    "Inter": "@clickstack_inter_unicode//file",
    "IBM+Plex+Mono": "@clickstack_ibm_plex_mono_unicode//file",
    "Roboto": "@clickstack_roboto_unicode//file",
    "Roboto+Mono": "@clickstack_roboto_mono_unicode//file",
}

def hyperdx_source_export(name, yarn_cache, yarn_manifest, fonts, font_manifest, visibility = None):
    """Build the ClickStack UI on Linux x86_64, regardless of the C++ target CPU.

    yarn_manifest: One JSON file next to cache/, with every Yarn zip SHA256.
    font_manifest: One JSON file next to CSS/WOFF inputs with SHA256 for both.
    yarn_cache and fonts contain all the files recorded in those manifests.
    These files must be fetched and pinned independently of the old static export.
    """
    js_run_binary(
        name = name,
        tool = "//bazel/rules/clickstack:build_hyperdx",
        srcs = [
            "@clickhouse_hyperdx_src//:sources",
            "@clickhouse_hyperdx_src//:package_json",
            "@clickstack_release_ignore//file",
            yarn_cache,
            yarn_manifest,
            fonts,
            font_manifest,
        ] + _FONT_COVERAGE.values(),
        out_dirs = [name + ".out"],
        args = [
            "../../../$(execpath @clickhouse_hyperdx_src//:package_json)",
            "../../../$(execpath %s)" % yarn_manifest,
            "../../../$(execpath %s)" % font_manifest,
            "../../../$(RULEDIR)/" + name + ".out",
            "../../../$(execpath @clickstack_release_ignore//file)",
        ] + [name + "=" + "../../../$(execpath %s)" % label for name, label in _FONT_COVERAGE.items()],
        # Keep the pinned external source files in the execroot. build_hyperdx
        # stages a private writable copy for Yarn and Next.
        copy_srcs_to_bin = False,
        exec_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:x86_64",
        ],
        mnemonic = "ClickStackNextExport",
        exec_properties = {"EstimatedMemory": "24GB", "EstimatedCPU": "8"},
        use_default_shell_env = False,
        visibility = visibility,
    )

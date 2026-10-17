"""Fetch ClickHouse's pinned third-party sources independently of Git submodules."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _shell_quote(value):
    return "'" + value.replace("'", "'\"'\"'") + "'"

def _cargo_sources_impl(ctx):
    # Cargo keeps the upstream relative path dependencies. Assemble only its
    # metadata layout; compilation still belongs to rules_rs repositories.
    for label, destination in ctx.attr.files.items():
        ctx.file(destination, ctx.read(label), executable = False)
    for label, destination in ctx.attr.directories.items():
        source = ctx.path(label).dirname
        if destination.startswith("contrib/"):
            ctx.symlink(source, destination)
        else:
            # Cargo canonicalizes manifest directories. A directory symlink
            # would resolve relative dependencies back into the user checkout.
            for entry in source.readdir():
                if entry.basename in ["Cargo.toml", "Cargo.lock"]:
                    ctx.file(destination + "/" + entry.basename, ctx.read(entry), executable = False)
                else:
                    ctx.symlink(entry, destination + "/" + entry.basename)
    ctx.file("BUILD.bazel", "")
    ctx.file("rust/workspace/BUILD.bazel", 'exports_files(["Cargo.toml", "Cargo.lock"])\n')
    ctx.file("bazel/rust/BUILD.bazel", 'exports_files(glob(["*/Cargo.toml", "*/Cargo.lock"]))\n')

_cargo_sources = repository_rule(
    implementation = _cargo_sources_impl,
    attrs = {
        "files": attr.label_keyed_string_dict(),
        "directories": attr.label_keyed_string_dict(),
    },
)

def _contrib_impl(module_ctx):
    repositories = json.decode(module_ctx.read(Label("//bazel:contrib_repositories.json")))
    for repository in repositories.values():
        if repository.get("module"):
            continue
        # The generated native rules preserve ClickHouse's configuration.
        # Remove upstream package boundaries and discovery exclusions so the
        # overlay sees the same inputs as ClickHouse's CMake configuration.
        # http_archive installs our root BUILD.bazel before patch_cmds run.
        remove = [".bazelignore"] + [
            path for path in repository["remove_build_files"] if path != "BUILD.bazel"
        ]
        http_archive(
            name = repository["name"],
            urls = [repository["url"]],
            sha256 = repository["sha256"],
            strip_prefix = repository["strip_prefix"],
            type = "tar.gz",
            build_file = Label("//bazel/dependencies:" + repository["name"] + ".BUILD.bazel"),
            # The upstream ../config.h lookup relies on an empty CMake auxdir.
            # Use the declared configuration include root in Bazel instead.
            patches = [Label("//bazel/patches:librdkafka-config-include.patch")] if repository["name"] == "clickhouse_librdkafka" else [],
            patch_args = ["-p1"],
            patch_cmds = ["rm -f -- " + " ".join([_shell_quote(path) for path in remove])] if remove else [],
        )
    _cargo_sources(
        name = "clickhouse_cargo_sources",
        files = {
            Label("//rust/workspace:Cargo.toml"): "rust/workspace/Cargo.toml",
            Label("//rust/workspace:Cargo.lock"): "rust/workspace/Cargo.lock",
        },
        directories = {
            Label("//rust/workspace:" + name + "/Cargo.toml"): "rust/workspace/" + name
            for name in ["skim", "prql", "polyglot", "pco", "wasmtime"]
        } | {
            Label("//bazel/rust:" + name + "/Cargo.toml"): "bazel/rust/" + name
            for name in ["chdig", "delta-kernel-rs"]
        } | {
            Label("@" + repositories["contrib/" + name]["name"] + "//:BUILD.bazel"): "contrib/" + name
            for name in ["chdig", "delta-kernel-rs", "pcodec", "wasmtime"]
        },
    )

contrib = module_extension(implementation = _contrib_impl)

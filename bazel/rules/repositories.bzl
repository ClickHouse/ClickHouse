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

    # rules_rs uses the lockfile's workspace root to select Cargo.toml features.
    # All actual crate pins come from ClickHouse's pinned Delta Kernel checkout.
    delta_lock = ctx.read(ctx.attr.delta_lock)
    delta_root = """
[[package]]
name = "clickhouse-delta-kernel-rs-dependencies"
version = "0.0.0"
dependencies = ["delta_kernel_ffi"]
"""
    if delta_root not in delta_lock:
        if '\nname = "clickhouse-delta-kernel-rs-dependencies"\n' in delta_lock:
            fail("Delta Kernel Cargo.lock already defines a different ClickHouse workspace root")
        if delta_lock.count('\nname = "delta_kernel_ffi"\n') != 1:
            fail("Delta Kernel Cargo.lock must pin a single delta_kernel_ffi version")
        delta_lock += delta_root
    ctx.file("bazel/rust/delta-kernel-rs/Cargo.lock", delta_lock, executable = False)
    ctx.file("BUILD.bazel", "")
    ctx.file("rust/workspace/BUILD.bazel", 'exports_files(["Cargo.toml", "Cargo.lock"])\n')
    ctx.file("bazel/rust/BUILD.bazel", 'exports_files(glob(["*/Cargo.toml", "*/Cargo.lock"]))\n')

_cargo_sources = repository_rule(
    implementation = _cargo_sources_impl,
    attrs = {
        "files": attr.label_keyed_string_dict(),
        "directories": attr.label_keyed_string_dict(),
        "delta_lock": attr.label(mandatory = True),
    },
)

def _openssl_configured_files(module_ctx):
    include = module_ctx.path(Label("//contrib/openssl-cmake:common/include/openssl/opensslv.h")).dirname.dirname
    files = {}
    for directory in include.readdir():
        for header in directory.readdir():
            if header.basename.endswith(".h"):
                relative = directory.basename + "/" + header.basename
                files["include/" + relative] = Label("//contrib/openssl-cmake:common/include/" + relative)
    for arch in ["x86_64", "aarch64"]:
        relative = "linux_" + arch + "/include_private/buildinf.h"
        files["configured/" + relative] = Label("//contrib/openssl-cmake:" + relative)
    return files

def _contrib_impl(module_ctx):
    repositories = json.decode(module_ctx.read(Label("//bazel/rules:contrib_repositories.json")))
    for repository in repositories.values():
        if repository.get("module"):
            continue

        # The generated native rules preserve ClickHouse's configuration.
        # Remove upstream package boundaries and discovery exclusions so the
        # overlay sees the same inputs as ClickHouse's CMake configuration.
        # http_archive installs our root BUILD.bazel before patch_cmds run.
        remove = [".bazelignore"] + [
            path
            for path in repository["remove_build_files"]
            if path != "BUILD.bazel"
        ]
        if repository["name"] == "clickhouse_rust_vendor":
            # Cargo checks every file listed in each vendored crate checksum.
            # Keep original BUILD files without adding Bazel package boundaries.
            # cmake_cargo.py restores these names in its declared-source checkout.
            commands = ["rm -f -- .bazelignore"] + [
                "mv -- " + _shell_quote(path) + " " + _shell_quote(path + ".clickhouse-cargo")
                for path in repository["remove_build_files"]
                if path != "BUILD.bazel"
            ]
        else:
            commands = ["rm -f -- " + " ".join([_shell_quote(path) for path in remove])] if remove else []
        http_archive(
            name = repository["name"],
            urls = [repository["url"]],
            sha256 = repository["sha256"],
            strip_prefix = repository["strip_prefix"],
            type = "tar.gz",
            build_file = Label("//bazel/dependencies:" + repository["name"] + ".BUILD.bazel"),
            files = _openssl_configured_files(module_ctx) if repository["name"] == "clickhouse_openssl" else {},
            patches = {
                "clickhouse_librdkafka": [Label("//bazel/patches:librdkafka-config-include.patch")],
                "clickhouse_mongo_cxx_driver": [Label("//bazel/patches:mongo-bsoncxx-enum-compatibility.patch")],
                "clickhouse_msgpack_c": [Label("//bazel/patches:msgpack-version-include.patch")],
            }.get(repository["name"], []),
            patch_args = ["-p1"],
            patch_cmds = commands,
        )
    _cargo_sources(
        name = "clickhouse_cargo_sources",
        delta_lock = Label("@" + repositories["contrib/delta-kernel-rs"]["name"] + "//:Cargo.lock"),
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
            Label("@" + repositories["contrib/" + name]["name"] + "//:Cargo.toml"): "contrib/" + name
            for name in ["chdig", "delta-kernel-rs", "pcodec", "wasmtime"]
        },
    )

contrib = module_extension(implementation = _contrib_impl)

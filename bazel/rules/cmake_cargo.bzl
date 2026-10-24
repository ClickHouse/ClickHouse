"""Build native ClickHouse Cargo archives from Bazel-declared Rust sources."""

load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("//bazel/rules:rust_archives.bzl", "RUST_ARCHIVES")
load("//bazel/rules:rust_cxx_flags.bzl", "RUST_CXX")

def _cmake_cargo_impl(ctx):
    rust = ctx.toolchains["@rules_rust//rust:toolchain_type"]
    if rust.target_flag_value not in ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu"]:
        fail("cmake_cargo supports the two native Linux Rust targets, got " + str(rust.target_flag_value))
    cc = find_cc_toolchain(ctx)
    features = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    archive = ctx.actions.declare_file(ctx.label.name + "/lib" + ctx.attr.cargo_package + ".a")
    evidence = ctx.actions.declare_file(ctx.label.name + "/cargo-evidence.json")
    args = ctx.actions.args()

    # A complete vendor input list exceeds argv limits. Python reads Bazel's
    # one-argument-per-line response file without shell interpretation.
    args.set_param_file_format("multiline")
    args.use_param_file("@%s", use_always = True)
    args.add("--output", archive)
    args.add("--evidence", evidence)
    args.add("--cargo", rust.cargo)
    args.add("--rustc", rust.rustc)
    args.add("--rust-sysroot-anchor", rust.sysroot_anchor)
    args.add("--target", rust.target_flag_value)
    args.add("--cargo-package", ctx.attr.cargo_package)
    args.add_all(ctx.attr.cargo_features, before_each = "--cargo-feature")
    args.add_all(ctx.attr.path_crates, before_each = "--path-crate")
    args.add("--config", ctx.file.config)
    args.add("--manifest", ctx.file.manifest)
    args.add("--workspace-manifest", ctx.file.workspace_manifest)
    for name, action in [
        ("cc", ACTION_NAMES.c_compile),
        ("cxx", ACTION_NAMES.cpp_compile),
        ("ar", ACTION_NAMES.cpp_link_static_library),
    ]:
        args.add("--" + name, cc_common.get_tool_for_action(
            feature_configuration = features,
            action_name = action,
        ))

    # MODULE.bazel supplies the target CPU, target features and v0 mangling.
    # Cargo adds its own package metadata; the driver adds the CMake sanitizer
    # metadata. Do not replace Cargo's package metadata with Bazel label hashes.
    args.add_all(rust.extra_rustc_flags, format_each = "--rust-flag=%s")
    if ctx.attr.native_source_root:
        args.add("--native-source-root", ctx.attr.native_source_root)
    args.add_all(["--mount", ctx.file.workspace_manifest.dirname, "rust/workspace"])

    # RUSTFLAGS uses the literal CMake sysroot spelling. Only stage the
    # target sysroot files already declared by this action.
    args.add_all(["--mount", ctx.attr.sysroot_files.label.workspace_root, "contrib/sysroot"])
    args.add_all(ctx.attr.sysroot_files[DefaultInfo].files, before_each = "--source")
    source_depsets = [ctx.attr.workspace_sources[DefaultInfo].files]
    args.add_all(ctx.attr.workspace_sources[DefaultInfo].files, before_each = "--source")
    destinations = {"contrib/sysroot": True}
    for target, destination in ctx.attr.repositories.items():
        if destination in destinations:
            fail("Duplicate cmake_cargo source destination: " + destination)
        destinations[destination] = True
        source_root = target.label.workspace_root
        if not source_root:
            fail("Cargo repositories must come from fetched source repositories: " + str(target.label))
        args.add_all(["--mount", source_root, destination])
        args.add_all(target[DefaultInfo].files, before_each = "--source")
        source_depsets.append(target[DefaultInfo].files)
    for required in ["contrib/rust_vendor"]:
        if required not in destinations:
            fail("cmake_cargo requires declared sources mounted at " + required)
    additional_inputs = []
    tools = []
    if rust.target_flag_value == "aarch64-unknown-linux-gnu":
        args.add("--native-linker", ctx.executable._native_linker)
        tools.append(ctx.executable._native_linker)

        # ClickHouse's pinned sysroot contains libgcc.a but no libgcc_s or
        # libutil. Use the registered LLVM runtime's own libgcc_s convention
        # and link its real libunwind archive.
        unwind = [file for file in ctx.files._native_unwind if file.extension == "a"]
        pic_unwind = [file for file in unwind if file.basename.endswith(".pic.a")]
        unwind = pic_unwind or unwind
        if len(unwind) != 1:
            fail("Expected one LLVM libunwind archive, got " + str(unwind))
        args.add("--native-unwind", unwind[0])
        args.add("--native-gcc-s", ctx.file._native_gcc_s)
        args.add("--native-glibc", ctx.file._native_glibc.path)
        additional_inputs.extend([unwind[0], ctx.file._native_gcc_s, ctx.file._native_glibc])
    if ctx.attr.cargo_package == "_ch_rust_prql":
        args.add("--contrib-repositories", ctx.file._contrib_repositories)
        additional_inputs.append(ctx.file._contrib_repositories)
    if ctx.attr.native_compilation:
        args.add("--native-compilation")
        arch = rust.target_flag_value.split("-")[0]
        args.add_all(RUST_CXX[arch]["ccopts"], format_each = "--native-c-flag=%s")
        args.add_all(RUST_CXX[arch]["copts"], format_each = "--native-cxx-flag=%s")
        native_headers = ctx.attr._native_headers[DefaultInfo].files
        args.add_all([
            "--mount",
            ctx.attr._native_headers.label.workspace_root,
            "contrib/llvm-project",
        ])
        args.add_all(native_headers, before_each = "--source")
        source_depsets.append(native_headers)
    if ctx.attr.openssl_headers:
        args.add("--openssl-headers", ctx.file.openssl_headers.path)
        additional_inputs.append(ctx.file.openssl_headers)
    if ctx.attr.cmake:
        args.add("--cmake", ctx.executable.cmake)
        tools.append(ctx.attr.cmake[DefaultInfo].files_to_run)
        if ctx.attr.cmake_files:
            source_depsets.append(ctx.attr.cmake_files[DefaultInfo].files)
    ctx.actions.run(
        executable = ctx.attr._driver[DefaultInfo].files_to_run,
        arguments = [args],
        inputs = depset(
            [ctx.file.config, ctx.file.manifest, ctx.file.workspace_manifest, rust.sysroot_anchor] + additional_inputs,
            transitive = source_depsets + [rust.all_files, cc.all_files, ctx.attr.sysroot_files[DefaultInfo].files],
        ),
        outputs = [archive, evidence],
        tools = tools,
        mnemonic = "ClickHouseCargo",
        progress_message = "Compiling native Cargo archive for %{label}",
        env = rust.env,
        execution_requirements = {"no-local": "1"} if ctx.attr.native_source_root else {},
    )
    return [
        DefaultInfo(files = depset([archive])),
        OutputGroupInfo(cargo_evidence = depset([evidence])),
    ]

cmake_cargo = rule(
    implementation = _cmake_cargo_impl,
    attrs = {
        "cargo_package": attr.string(
            mandatory = True,
            values = [spec["package"] for spec in RUST_ARCHIVES.values()],
        ),
        "cargo_features": attr.string_list(),
        # C/C++ code compiled by Cargo build scripts needs the ClickHouse
        # flags from the reference and the original libc++ source headers.
        "native_compilation": attr.bool(),
        "openssl_headers": attr.label(allow_single_file = True),
        "cmake": attr.label(executable = True, cfg = "exec", allow_files = True),
        "cmake_files": attr.label(allow_files = True, cfg = "exec"),
        "_contrib_repositories": attr.label(
            default = "//bazel/rules:contrib_repositories.json",
            allow_single_file = True,
        ),
        # Additional Cargo path crates to record in the raw archive evidence.
        # The package being compiled is always recorded too.
        "path_crates": attr.string_list(),
        "manifest": attr.label(mandatory = True, allow_single_file = ["Cargo.toml"]),
        "workspace_manifest": attr.label(mandatory = True, allow_single_file = ["Cargo.toml"]),
        "workspace_sources": attr.label(mandatory = True, allow_files = True),
        # Keys are complete source filegroups; values are their directories in
        # ClickHouse, such as contrib/pcodec. Original Cargo.toml paths remain
        # valid when Cargo resolves the complete ClickHouse Rust workspace.
        "repositories": attr.label_keyed_string_dict(mandatory = True, allow_files = True),
        "config": attr.label(mandatory = True, allow_single_file = True),
        "sysroot_files": attr.label(default = "@clickhouse_sysroot//:linux_compile_files"),
        # Cargo hashes the absolute SourceId of path dependencies outside
        # rust/workspace. Use the recorded CMake source root in an isolated RBE
        # action so the pinned Cargo recomputes those SourceIds itself.
        "native_source_root": attr.string(),
        "_native_linker": attr.label(default = "@llvm//tools:ld.lld", executable = True, cfg = "exec"),
        "_native_unwind": attr.label(default = "@llvm//runtimes/libunwind:libunwind.static", cfg = "exec"),
        "_native_gcc_s": attr.label(default = "@llvm//runtimes/libunwind:gcc_s", allow_single_file = True, cfg = "exec"),
        "_native_glibc": attr.label(default = "@llvm//runtimes/glibc:glibc_library_search_directory", allow_single_file = True, cfg = "exec"),
        "_native_headers": attr.label(default = "@clickhouse_llvm_project//:runtime_libcxx_headers"),
        "_driver": attr.label(default = "//bazel/rules:cmake_cargo", executable = True, cfg = "exec"),
    },
    fragments = ["cpp"],
    toolchains = use_cc_toolchain() + ["@rules_rust//rust:toolchain_type"],
)

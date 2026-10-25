"""Declared license rendering and copies of source files read by Clang #embed."""

def rust_vendor_licenses(name, filenames):
    """Select the first license filename used by process_rust_crate for each crate."""
    selected = {}
    for filename in filenames:
        # A wildcard makes glob return the original spelling of LICENSE.txt and LICENSE.TXT on macOS.
        for path in native.glob(["*/" + filename + "*"], allow_empty = True):
            crate, found = path.split("/", 1)
            if found == filename:
                selected.setdefault(crate, path)
    native.filegroup(name = name, srcs = sorted(selected.values()))

def _add_license(files, original, source):
    if original in files or not source.is_source:
        fail("Expected one source file for license %s" % original)
    files[original] = source

def _licenses_impl(ctx):
    if len(ctx.files.srcs) != len(ctx.attr.source_paths):
        fail("Each source_paths entry must have exactly one input file in srcs")
    files = {}
    for original, source in zip(ctx.attr.source_paths, ctx.files.srcs):
        _add_license(files, original, source)
    vendor = ctx.attr._rust_vendor_licenses.label
    for source in ctx.files._rust_vendor_licenses:
        if not source.owner or source.owner.repo_name != vendor.repo_name or source.owner.package:
            fail("Expected a source from the root package of %s: %s" % (vendor, source))
        _add_license(files, "contrib/rust_vendor/" + source.owner.name, source)
    if len(depset(files.values()).to_list()) != len(files):
        fail("A license file has more than one source path")
    originals = sorted(files)
    inputs = [files[original] for original in originals] + [ctx.file.manifest]
    args = ctx.actions.args()
    args.add(ctx.file.manifest)
    args.add(ctx.outputs.out)
    for original in originals:
        args.add(original)
        args.add(files[original])
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
        "_rust_vendor_licenses": attr.label(default = Label("@clickhouse_rust_vendor//:licenses")),
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

def cpp_proto_sources(src_dir, out_dir, imports):
    """Generate a .pb.h/.pb.cc pair for each schema and its transitive imports."""
    protoc = "@clickhouse_google_protobuf//:protoc"
    for stem, imported in imports.items():
        src = src_dir + "/" + stem + ".proto"
        native.genrule(
            name = stem + "_cpp_sources",
            srcs = sorted([src] + [src_dir + "/" + dep + ".proto" for dep in imported]),
            outs = [out_dir + "/" + stem + ext for ext in [".pb.h", ".pb.cc"]],
            tools = [protoc],
            cmd = ('mkdir -p "$(RULEDIR)/{out}" && ' +
                   '"$(execpath {protoc})" --cpp_out="$(RULEDIR)/{out}" ' +
                   '-I "$$(dirname "$(execpath :{src})")" "$(execpath :{src})"').format(
                out = out_dir,
                protoc = protoc,
                src = src,
            ),
        )

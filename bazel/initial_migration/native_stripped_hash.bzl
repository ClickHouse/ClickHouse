"""Compare a completed native CMake reference and the Bazel executable."""

load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")

def _native_stripped_hash_impl(ctx):
    reference = ctx.files.reference
    if len(reference) == 1 and reference[0].is_directory:
        directory = reference[0].path
    elif (sorted([file.basename for file in reference]) == ["clickhouse.cmake", "cmake-reference-summary.json", "status.json"] and
          all([not file.is_directory and file.dirname == reference[0].dirname for file in reference])):
        directory = reference[0].dirname
    else:
        fail("reference must be the native CMake output directory or its clickhouse.cmake, cmake-reference-summary.json and status.json files")
    cc = find_cc_toolchain(ctx)
    features = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    evidence = ctx.actions.declare_directory(ctx.label.name)
    args = ctx.actions.args()
    args.add("--reference", directory)
    args.add("--clickhouse", ctx.file.clickhouse)
    args.add("--output", evidence.path)
    for name, action in [
        ("cc", ACTION_NAMES.c_compile),
        ("cxx", ACTION_NAMES.cpp_compile),
    ]:
        args.add("--" + name, cc_common.get_tool_for_action(
            feature_configuration = features,
            action_name = action,
        ))
    ctx.actions.run(
        executable = ctx.attr._driver[DefaultInfo].files_to_run,
        arguments = [args],
        inputs = depset(
            reference + [ctx.file.clickhouse],
            transitive = [cc.all_files],
        ),
        outputs = [evidence],
        mnemonic = "ClickHouseStrippedParity",
        progress_message = "Checking native CMake and Bazel ClickHouse stripped hashes",
        execution_requirements = {"no-local": "1"},
    )
    return [DefaultInfo(files = depset([evidence]))]

native_stripped_hash = rule(
    implementation = _native_stripped_hash_impl,
    attrs = {
        "reference": attr.label(mandatory = True, allow_files = True),
        "clickhouse": attr.label(mandatory = True, allow_single_file = True),
        "_driver": attr.label(
            default = "//bazel/initial_migration:native_stripped_hash_driver",
            executable = True,
            cfg = "exec",
        ),
    },
    fragments = ["cpp"],
    toolchains = use_cc_toolchain(),
)

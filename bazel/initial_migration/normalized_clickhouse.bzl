"""Expose the recorded CMake normalization as a single Bazel output file."""

load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")

def _normalized_clickhouse_impl(ctx):
    cc = find_cc_toolchain(ctx)
    features = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    compiler = cc_common.get_tool_for_action(
        feature_configuration = features,
        action_name = ACTION_NAMES.cpp_compile,
    )
    normalized = ctx.actions.declare_file(ctx.label.name)
    args = ctx.actions.args()
    args.add(compiler)
    args.add(ctx.file.clickhouse)
    args.add(normalized)
    args.add(ctx.attr.compiler_sha256)
    args.add(ctx.attr.elf_machine)
    ctx.actions.run_shell(
        arguments = [args],
        inputs = depset([ctx.file.clickhouse], transitive = [cc.all_files]),
        outputs = [normalized],
        command = """
set -eu
compiler="$1"
original="$2"
normalized="$3"
test "$(sha256sum "$compiler" | cut -d' ' -f1)" = "$4"
test "$(od -An -tx1 -N6 "$original" | tr -d ' \\n')" = 7f454c460201
test "$(od -An -tu2 -j18 -N2 "$original" | tr -d ' \\n')" = "$5"
case "$compiler" in
    /*) compiler_file="$compiler" ;;
    *) compiler_file="$PWD/$compiler" ;;
esac
scratch="$(mktemp -d)"
trap 'rm -rf -- "$scratch"' EXIT
ln -s "$compiler_file" "$scratch/llvm-strip"
"$scratch/llvm-strip" --strip-all --remove-section=.note.gnu.build-id -o "$normalized" "$original"
""",
        mnemonic = "ClickHouseNormalize",
        progress_message = "Strip ClickHouse with the recorded CMake compiler and flags",
        execution_requirements = {"no-local": "1"},
    )
    return [DefaultInfo(files = depset([normalized]))]

normalized_clickhouse = rule(
    implementation = _normalized_clickhouse_impl,
    attrs = {
        "clickhouse": attr.label(default = "//:clickhouse", allow_single_file = True),
        "compiler_sha256": attr.string(mandatory = True),
        "elf_machine": attr.int(mandatory = True),
    },
    fragments = ["cpp"],
    toolchains = use_cc_toolchain(),
)

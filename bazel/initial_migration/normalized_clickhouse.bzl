"""Strip a Bazel ClickHouse binary with the options used for the CMake reference."""

def _normalized_clickhouse_impl(ctx):
    normalized = ctx.actions.declare_file(ctx.label.name)
    args = ctx.actions.args()
    args.add(ctx.executable._llvm_strip)
    args.add(ctx.file.clickhouse)
    args.add(normalized)
    args.add(ctx.attr.elf_machine)
    ctx.actions.run_shell(
        arguments = [args],
        inputs = [ctx.file.clickhouse],
        tools = [ctx.attr._llvm_strip[DefaultInfo].files_to_run],
        outputs = [normalized],
        command = """
set -eu
strip="$1"
original="$2"
normalized="$3"
test "$(od -An -tx1 -N6 "$original" | tr -d ' \\n')" = 7f454c460201
test "$(od -An -tu2 -j18 -N2 "$original" | tr -d ' \\n')" = "$4"
"$strip" --strip-all --remove-section=.note.gnu.build-id -o "$normalized" "$original"
""",
        mnemonic = "ClickHouseNormalize",
        progress_message = "Strip ClickHouse with llvm-strip",
        execution_requirements = {"no-local": "1"},
    )
    return [DefaultInfo(files = depset([normalized]))]

normalized_clickhouse = rule(
    implementation = _normalized_clickhouse_impl,
    attrs = {
        "clickhouse": attr.label(default = "//:clickhouse", allow_single_file = True),
        "elf_machine": attr.int(mandatory = True),
        "_llvm_strip": attr.label(
            default = "@llvm//tools:llvm-strip",
            allow_files = True,
            executable = True,
            cfg = "exec",
        ),
    },
)

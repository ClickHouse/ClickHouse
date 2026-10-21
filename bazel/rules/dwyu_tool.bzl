"""Build DWYU executables with the complete LLVM runtime on their selected executor."""

_RUNTIME_STAGE = str(Label("@llvm//toolchain:runtime_stage"))

def _complete_runtime_impl(_settings, _attr):
    return {_RUNTIME_STAGE: "complete"}

_complete_runtime = transition(
    implementation = _complete_runtime_impl,
    inputs = [],
    outputs = [_RUNTIME_STAGE],
)

def _dwyu_tool_impl(ctx):
    binary = ctx.attr.binary[DefaultInfo]
    executable = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.symlink(output = executable, target_file = binary.files_to_run.executable, is_executable = True)
    return [DefaultInfo(
        executable = executable,
        runfiles = binary.default_runfiles,
    )]

dwyu_tool = rule(
    implementation = _dwyu_tool_impl,
    cfg = _complete_runtime,
    executable = True,
    attrs = {
        "binary": attr.label(
            cfg = "target",
            executable = True,
            mandatory = True,
        ),
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
)

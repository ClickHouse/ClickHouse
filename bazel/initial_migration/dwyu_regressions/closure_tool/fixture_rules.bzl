"""Only migration tests reset Bazel's pinned exec flag after a real exec edge."""

load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")

def _exec_root_impl(ctx):
    return [DefaultInfo(files = ctx.attr.tool[DefaultInfo].files)]

fixture_exec_root = rule(
    implementation = _exec_root_impl,
    attrs = {"tool": attr.label(cfg = "exec", mandatory = True)},
)

# Bazel 9.3.0 CoreOptions.isExec is the source of cquery's isTool field.
# A normal cfg = "target" below an exec edge leaves it true. Reset it only
# here to exercise a non-tool descendant with a different original checksum.
def _back_to_cpp_impl(settings, attr):
    return {"//command_line_option:is exec configuration": False}

_back_to_cpp = transition(
    implementation = _back_to_cpp_impl,
    inputs = [],
    outputs = ["//command_line_option:is exec configuration"],
)

def _return_cpp_impl(ctx):
    owner = ctx.attr.actual[0]
    return [owner[CcInfo], DefaultInfo(files = owner[DefaultInfo].files)]

fixture_return_cpp = rule(
    implementation = _return_cpp_impl,
    attrs = {
        "actual": attr.label(cfg = _back_to_cpp, mandatory = True, providers = [CcInfo]),
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
)

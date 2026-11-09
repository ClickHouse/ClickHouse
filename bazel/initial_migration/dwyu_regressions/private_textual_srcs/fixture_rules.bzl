"""A generated source directory for private_textual_srcs validation."""

def _source_tree_impl(ctx):
    directory = ctx.actions.declare_directory(ctx.label.name + ".cpp")
    ctx.actions.run_shell(
        arguments = [directory.path],
        command = 'mkdir -p "$1" && printf "%s\n" "int pts_tree_value = 3;" > "$1/fragment.cpp"',
        outputs = [directory],
    )
    return [DefaultInfo(files = depset([directory]))]

source_tree = rule(implementation = _source_tree_impl)

"""Stage complete directories from pinned Cargo repositories without changing Cargo's file list."""

_CargoSourceInputs = provider(fields = ["files"])

def cargo_source_inputs(target):
    """Return the action inputs for a Cargo source target."""
    return target[_CargoSourceInputs].files if _CargoSourceInputs in target else target[DefaultInfo].files

def _cargo_sources_impl(ctx):
    sources = depset(ctx.files.srcs)
    return [
        DefaultInfo(files = sources),
        _CargoSourceInputs(files = depset(ctx.files.directory_inputs) if ctx.attr.directory_inputs else sources),
    ]

_cargo_sources = rule(
    implementation = _cargo_sources_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "directory_inputs": attr.label_list(allow_files = True),
    },
)

def cargo_repository_sources(name, root = "", exclude_top_level = [], split_directories = [], **kwargs):
    """Publish individual filenames for Cargo's response file and directories for action inputs."""
    pattern = root + "/**" if root else "**"
    sources = native.glob(
        [pattern],
        exclude = ["BUILD", "BUILD.bazel"] + [d + "/**" for d in exclude_top_level],
    )
    inputs = {}

    # A directory input would cross a nested package which glob() omitted.
    if sources and not native.subpackages(include = [pattern], allow_empty = True):
        if root:
            inputs[root] = True
        else:
            for source in sources:
                first, _, rest = source.partition("/")
                if first in split_directories and rest:
                    first = first + "/" + rest.partition("/")[0]
                inputs[first] = True
    _cargo_sources(
        name = name,
        srcs = sources,
        directory_inputs = inputs.keys(),
        **kwargs
    )

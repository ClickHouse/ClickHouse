"""Find the source or generated external repository directory used by Clang and Cargo."""

load("@bazel_skylib//lib:paths.bzl", "paths")

def link_runtime_root(target):
    """Return the directory containing every linux-<arch> file in target."""
    roots = {paths.join(f.root.path, f.owner.workspace_root): True for f in target.files.to_list()}
    if len(roots) != 1:
        fail("{} must provide one link runtime directory, got {}".format(target.label, roots.keys()))
    return roots.keys()[0]

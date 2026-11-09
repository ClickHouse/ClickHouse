"""Run the configured analyzer with previously verified DWYU preprocessing outputs."""

load("//bazel/rules:dwyu.bzl", "dwyu")

def _report(ctx):
    owner = ctx.attr.owner
    tool = ctx.attr._analyzer[DefaultInfo].files_to_run
    support = owner[OutputGroupInfo].dwyu_analyzer_inputs
    blobs = {f.basename: f.path for f in ctx.files.observations}
    if len(blobs) != len(ctx.files.observations):
        fail("duplicate observation digest in {}".format(ctx.label))
    if ctx.target_platform_has_constraint(ctx.attr._x86[platform_common.ConstraintValueInfo]):
        arch = "x86"
    elif ctx.target_platform_has_constraint(ctx.attr._arm[platform_common.ConstraintValueInfo]):
        arch = "arm"
    else:
        fail("unsupported DWYU target architecture")
    label = str(owner.label)
    manifest = ctx.actions.declare_file(ctx.label.name + ".inputs.json")
    report = ctx.actions.declare_file(ctx.label.name + ".json")
    ctx.actions.write(manifest, json.encode({
        "arch": arch,
        "blobs": blobs,
        "exe": tool.executable.path,
        "owner": label[2:] if label.startswith("@@//") else label,
        "support": [f.path for f in support.to_list()],
    }))
    args = ctx.actions.args()
    args.add_all([ctx.file.plan, ctx.attr.plan_sha256, manifest, report])
    ctx.actions.run(
        executable = ctx.executable._driver,
        tools = [tool],
        inputs = depset([ctx.file.plan, manifest] + ctx.files.observations, transitive = [support]),
        outputs = [report],
        arguments = [args],
        mnemonic = "DwyuPartitionAnalyzeTarget",
    )
    return [DefaultInfo(files = depset([report])), OutputGroupInfo(dwyu_partition_report = depset([report]))]

dwyu_partition_report = rule(
    implementation = _report,
    attrs = {
        # Bazel passes these rule attributes to the original DWYU aspect.
        "dwyu_analysis_ignores_private_headers_from_deps": attr.bool(default = True),
        "dwyu_analysis_optimizes_impl_deps": attr.bool(default = True),
        "dwyu_analysis_reports_missing_direct_deps": attr.bool(default = True),
        "dwyu_analysis_reports_unused_deps": attr.bool(default = True),
        "dwyu_verbose": attr.bool(default = False),
        "owner": attr.label(aspects = [dwyu], mandatory = True),
        "observations": attr.label_list(allow_files = True, mandatory = True),
        "plan": attr.label(allow_single_file = True, mandatory = True),
        "plan_sha256": attr.string(mandatory = True),
        "_analyzer": attr.label(default = "//bazel/rules:dwyu_analyze_includes", executable = True, cfg = "exec"),
        "_driver": attr.label(default = "//bazel/initial_migration:dwyu_partition", executable = True, cfg = "exec"),
        "_arm": attr.label(default = "@platforms//cpu:aarch64"),
        "_x86": attr.label(default = "@platforms//cpu:x86_64"),
    },
)

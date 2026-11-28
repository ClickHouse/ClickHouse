"""Check declared private textual C/C++ sources within explicit configured roots."""

load("@rules_cc//cc/common:cc_helper.bzl", "cc_helper")
load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")

_SourceCensusInfo = provider(
    "Configured C/C++ declarations reached via deps and implementation_deps.",
    fields = {
        "identity": "Path of an artifact owned by the configured target.",
        "records": "Depset of JSON records describing declarations and unsupported artifacts.",
    },
)
_SOURCE_ATTRIBUTES = ["deps", "implementation_deps"]
_CC_KINDS = ["cc_library", "cc_binary", "cc_test"]

def _collect_sources(target, ctx):
    # A declared artifact identifies Bazel's configured target, including
    # transitions. Match the identity convention in dwyu_owner_graph_impl.
    identity = ctx.actions.declare_file("{}.dwyu_textual_srcs_owner".format(target.label.name))
    ctx.actions.write(identity, str(target.label))
    compiled = []
    textual = []
    problems = []
    transitive = []
    for attribute in _SOURCE_ATTRIBUTES:
        for dep in getattr(ctx.rule.attr, attribute, []):
            if _SourceCensusInfo in dep:
                transitive.append(dep[_SourceCensusInfo].records)
            else:
                problems.append({
                    "reason": "unvisited_dependency",
                    "attribute": attribute,
                    "dependency": str(dep.label),
                })

    if ctx.rule.kind not in _CC_KINDS or CcInfo not in target:
        problems.append({
            "reason": "unsupported_rule_kind",
            "kind": ctx.rule.kind,
        })
    else:
        extensions = cc_helper.extensions
        code = extensions.C_SOURCE + extensions.CC_SOURCE
        known_non_code = extensions.CC_HEADER + extensions.CC_TEXTUAL_INCLUDE + extensions.ASSEMBLER + extensions.ASSEMBLER_WITH_C_PREPROCESSOR
        for file in getattr(ctx.rule.files, "srcs", []):
            extension = "." + file.extension
            if file.is_directory:
                problems.append({"reason": "srcs_tree_artifact", "file": file.path})
            elif extension in code:
                compiled.append(file.path)
            elif extension not in known_non_code:
                # An object, archive, module or unrecognized artifact is not
                # evidence about the source files compiled to create it.
                problems.append({"reason": "unsupported_srcs_artifact", "file": file.path})
        for file in getattr(ctx.rule.files, "private_textual_srcs", []):
            if file.is_directory:
                problems.append({"reason": "private_textual_srcs_tree_artifact", "file": file.path})
            elif "." + file.extension not in code:
                problems.append({"reason": "invalid_private_textual_srcs_extension", "file": file.path})
            else:
                textual.append(file.path)

    record = json.encode({
        "label": str(target.label),
        "configured_owner": identity.path,
        "kind": ctx.rule.kind,
        "compiled": sorted(compiled),
        "private_textual_srcs": sorted(textual),
        "problems": problems,
    })
    return [_SourceCensusInfo(
        identity = identity.path,
        records = depset(direct = [record], transitive = transitive),
    )]

_source_census = aspect(
    implementation = _collect_sources,
    attr_aspects = _SOURCE_ATTRIBUTES,
    provides = [_SourceCensusInfo],
)

def _add_owner(table, file, owner):
    if file not in table:
        table[file] = []
    table[file].append(owner)

def _source_census_impl(ctx):
    roots = []
    transitive = []
    problems = []
    for root in ctx.attr.roots:
        if _SourceCensusInfo not in root:
            problems.append({"reason": "unvisited_root", "label": str(root.label)})
            continue
        info = root[_SourceCensusInfo]
        roots.append({"label": str(root.label), "configured_owner": info.identity})
        transitive.append(info.records)
    if not roots:
        problems.append({"reason": "no_configured_roots"})

    compiled = {}
    textual = {}
    records = []
    for encoded in sorted(depset(transitive = transitive).to_list()):
        record = json.decode(encoded)
        records.append(record)
        owner = {"label": record["label"], "configured_owner": record["configured_owner"]}
        for file in record["compiled"]:
            _add_owner(compiled, file, owner)
        for file in record["private_textual_srcs"]:
            _add_owner(textual, file, owner)
        for problem in record["problems"]:
            problems.append(dict(problem, owner = owner))

    conflicts = []
    for file in sorted(textual):
        claims = textual[file]

        # Two configurations of one label remain separate in the report, but
        # are not declarations by different libraries.
        claimant_labels = {owner["label"]: True for owner in claims}
        if len(claimant_labels) > 1:
            conflicts.append({
                "reason": "private_textual_source_claimed_by_multiple_owners",
                "file": file,
                "claimants": claims,
            })
        for compiler in compiled.get(file, []):
            other_claims = [owner for owner in claims if owner["label"] != compiler["label"]]
            if other_claims:
                conflicts.append({
                    "reason": "textual_source_compiled_by_other_owner",
                    "file": file,
                    "claimants": other_claims,
                    "compiler": compiler,
                })

    passed = not conflicts and not problems
    status = "verified_within_scope" if passed else ("conflict" if conflicts else "held")
    report = ctx.actions.declare_file(ctx.label.name + ".census.json")
    ctx.actions.write(report, json.encode_indent({
        "audit_target": str(ctx.label),
        "status": status,
        "scope": {
            "roots": roots,
            "traversed_attributes": _SOURCE_ATTRIBUTES,
            "statement": "Only configured targets reached from these roots through deps and implementation_deps were examined. No ownership claim is made outside these roots.",
        },
        "configured_targets": records,
        "conflicts": conflicts,
        "unsupported": problems,
    }, indent = "  ") + "\n")
    check = ctx.actions.declare_file(ctx.label.name + ".census.check")
    if passed:
        ctx.actions.write(check, "verified_within_scope\n")
    else:
        # Report creation succeeds independently. A failed default build prints
        # the full report; --output_groups=report retrieves it without the check.
        ctx.actions.run_shell(
            inputs = [report],
            outputs = [check],
            arguments = [report.path],
            command = 'cat "$1" >&2; exit 1',
            mnemonic = "DwyuCheckTextualSrcsCensus",
            progress_message = "Check private_textual_srcs ownership for {}".format(ctx.label),
        )
    return [
        DefaultInfo(files = depset([check, report])),
        OutputGroupInfo(report = depset([report])),
    ]

dwyu_textual_srcs_census = rule(
    implementation = _source_census_impl,
    attrs = {
        "roots": attr.label_list(
            aspects = [_source_census],
            allow_files = False,
            mandatory = True,
            doc = "Configured roots whose deps and implementation_deps closures are checked. No targets outside these closures are covered.",
        ),
    },
)

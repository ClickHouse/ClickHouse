"""Verify selected DWYU public-header preprocessing, independently of C++ and link coverage."""

import hashlib
import json
from pathlib import Path
import re

from audit_full_dwyu import (
    DWYU_ASPECT, DWYU_PARAMETERS, FIELDS, RuntimeActionGraph, _RUNTIME_PP_SHELL,
    analyzer_arguments, analyzer_option, bep_file_matches, canonical, dwyu_request, identity, select_completions,
)


# A successful DwyuProcessSourceFile is evidence of physical entry only with the
# extractor that checks the stdin include against Clang's header trace. Recheck
# that contract before trusting other DWYU, rules_cc or toolchain definitions.
SOURCE_SHA256 = {
    ".bazelversion": "14ebdd5f5d8dfccb0fc574ea9aabecc07d7cdd99d53e4fc01e58f9c3d23d18ef",
    "MODULE.bazel": "a6ccee15f48a4977fae32285ca9b759a50d06470b6a540404d839f56d12f8de1",
    "bazel/rules/dwyu.bzl": "d9bcd9782eb753ca8287bbb7698216bfd56251fc0c35adebb05edaf3d9b1637f",
    "bazel/rules/dwyu_tool.bzl": "9fbe5b5759ed86eb74c46f2bf753dd922cbf767fadb6fc52b9ca7e09ce38260f",
    "bazel/rules/BUILD.bazel": "5419adcf5cedb4752e8a4303bb5b413115b96c3bdd234b2cf11302d3ea457b81",
    "bazel/patches/dwyu-compiler-preprocessing.patch": "7b120d8fd4db2fb24bb658f55bf456025457ee6541a4eb779c4c850e97f003cb",
    "bazel/patches/rules_cc-implementation-deps-local-includes.patch": "1e8f54793cf1989dc11017982e75bd18f22bd09fc89d077c7749edcc146b6714",
    "bazel/patches/llvm-clickhouse-runtime.patch": "ffc1537fb817152a7651ea8ef1007c3799b46b496c8dcc8f6c05eac2836a1a48",
}
REGISTRY_SHA256 = {
    "depend_on_what_you_use/1.1.2/MODULE.bazel": "dc739111b70e715d10ca13fdd69c5da30af8f64a61e6dce0887be5e9a0d70cf9",
    "depend_on_what_you_use/1.1.2/source.json": "5dad1b12896af1ef9ab5ecb86c31ac2c6fbb6d03871652eeb3a2c1afef382c2f",
    "llvm/0.8.24/MODULE.bazel": "e03e030b1d0b44bb05c52665c2f43de9260af9152f666262e11e25f6c072aae3",
    "llvm/0.8.24/source.json": "924ed48858c1c081698f1992c7b489434375de901c47b7046e203459840cb544",
    "rules_cc/0.2.26/MODULE.bazel": "a9f7b7f4a52747910965bddb1f58fc1fef50604ed1980ebdd16ea6f446d92249",
    "rules_cc/0.2.26/source.json": "bf9ff315da6055c1e374d627012cc51947a05b3560674af7d69fbedbb4fde6ef",
}
MAX_OWNERS = 8
MAX_HEADERS = 32  # across the entire aquery, after expanding globs and virtual aliases
MAX_GRAPH_BYTES = 32 << 20
PP = "DwyuCompilerPreprocess"
PROCESS = "DwyuProcessSourceFile"
ANALYZE = "DwyuAnalyzeTarget"
# Bazel records both required aspects alongside the aspect that registered the action.
ASPECTS = (DWYU_ASPECT + "dwyu", "@@depend_on_what_you_use+//dwyu/cc/aspect:factory.bzl%_dwyu_import_metadata",
           DWYU_ASPECT + "_rust_archive_scans")
SHELL = _RUNTIME_PP_SHELL.replace('"$compiler" "$@"', r'''directive="$1"; shift; printf '%s\n' "$directive" | "$compiler" "$@"''')

# The same config(..., target) query already checks IncompatiblePlatformProvider.
# Emit just the include directories that could spell one of these public Files.
PROVIDERS = '''
def header_provider(p):
    keys = [k for k in p if k == "CcInfo" or k.endswith("%CcInfo")]
    if keys not in [["@@rules_cc+//cc/private:cc_info.bzl%CcInfo"], ["@@rules_cc+0.2.26//cc/private:cc_info.bzl%CcInfo"]]:
        return {"error": "unknown CcInfo"}
    cc = p[keys[0]]
    c = cc.compilation_context
    if not c.direct_public_headers and not c.direct_private_headers and c.direct_textual_headers and len(c.direct_textual_headers) <= MAX_FILES:
        if not any([f.is_directory for f in c.direct_textual_headers]):
            return {"textual_files": [f.path for f in c.direct_textual_headers]}
    if not c.direct_public_headers or len(c.direct_public_headers) > MAX_FILES or c.direct_private_headers or c.direct_textual_headers:
        return {"error": "expected a small set of modular public headers with no private or textual headers"}
    paths = []
    prefixes = {".": True}
    for f in c.direct_public_headers:
        if f.is_directory:
            return {"error": "public header is a tree artifact"}
        paths.append(f.path)
        parts = f.path.split("/")
        for i in range(1, len(parts)):
            prefixes["/".join(parts[:i])] = True
    groups = [c.includes.to_list() + c.system_includes.to_list() + c.external_includes.to_list(), c.quote_includes.to_list()]
    dirs = [sorted({(p or "."): True for p in group if (p or ".") in prefixes}.keys()) for group in groups]
    has_link_inputs = False
    for entry in cc.linking_context.linker_inputs.to_list():
        for field in ["libraries", "user_link_flags", "linkstamps", "additional_inputs"]:
            v = getattr(entry, field, [])
            if (v.to_list() if type(v) == "depset" else v):
                has_link_inputs = True
                break
        if has_link_inputs:
            break
    return {"public": paths, "include_dirs": dirs, "has_link_inputs": has_link_inputs}

def format(target):
    p = providers(target)
    label = str(target.label)
    result = {"label": label, "incompatible": None if p == None else "IncompatiblePlatformProvider" in p}
    if label in SELECTED and result["incompatible"] == False:
        result["standalone_headers"] = header_provider(p)
    return json.encode(result)
'''


def require(condition, message):
    if not condition:
        raise ValueError(message)


def provider_query(labels):
    """Use actual canonical labels, including the main repository's @@ spelling."""
    names = sorted({n for label in labels for n in (label, "@@" + label if label.startswith("//") else label)})
    return "SELECTED = " + repr(names) + "\nMAX_FILES = " + str(2 * MAX_HEADERS) + "\n" + PROVIDERS


def candidates(rows, labels):
    eligible = []
    for row in rows.values():
        if row["label"] not in labels or "standalone_headers" not in row:
            continue
        cc = row["standalone_headers"]
        srcs = row["sources"]
        if (row["rule_class"] != "cc_library" or row.get("is_tool") is not False or srcs["srcs"]
                or row.get("extra_compiler_inputs") != 0 or cc.get("error")):
            continue
        if srcs["hdrs"] and not srcs["textual_hdrs"] and "public" in cc:
            paths = cc["public"]
        elif srcs["textual_hdrs"] and not srcs["hdrs"] and "textual_files" in cc:
            paths = cc["textual_files"]
        else:
            continue
        require(0 < len(paths) <= 2 * MAX_HEADERS and len(set(paths)) == len(paths), "invalid CcInfo header Files")
        eligible.append((row["label"], len(paths)))
    # Count physical and virtual Files before Bazel expands any compiler actions.
    require(sum(n for _, n in eligible) <= MAX_HEADERS, "selected CcInfo Files exceed the standalone header limit")
    return sorted(label for label, _ in eligible)


def report_stem(path):
    suffix = "_dwyu_report.json"
    return path[:-len(suffix)] if path.endswith(suffix) else path


def add_outputs(expr, reports):
    # Bazel 9.3 applies action filters at the outermost query. A union of two
    # filtered expressions can emit every action on every selected target.
    match = re.fullmatch(r"outputs\('([^'\n]+)', (set\([^'\n]+\))\)", expr)
    require(match and all(p.endswith("_dwyu_report.json") and "'" not in p for p in reports),
            "unexpected configured DWYU output filter")
    stems = "|".join(sorted(re.escape(report_stem(p)) for p in reports))
    outputs = "^(" + stems + ")[.]dwyu_(pp|psf)_[^/]+$"
    return "outputs('(" + match[1] + ")|" + outputs + "', " + match[2] + ")"


def header_aliases(cc, manifest):
    public = cc["public"]
    owned = manifest["contents"]
    require(not any(owned.get(k) for k in ("private", "required", "raw_assembly", "raw_link_candidates",
                                            "unchecked_link_inputs", "unverified_compiler_inputs")),
            "owner has headers or inputs outside standalone preprocessing")
    mapping = owned["public"]
    require(set(public) == set(mapping) and type(cc["has_link_inputs"]) is bool, "CcInfo/manifest public Files differ")
    aliases = {}
    i = 0
    while i < len(public):
        alias = public[i]
        original = mapping[alias]
        require(all(isinstance(p, str) and not any(v in p for v in ('"', "<", ">", "\r", "\n", "\\"))
                    and all(part not in ("", ".", "..") for part in p.split("/")) for p in (alias, original)),
                "invalid public File path")
        if alias != original:
            i += 1
            require(i < len(public) and public[i] == original, "virtual File must precede its physical File")
        require(mapping.get(original) == original and original not in aliases, "ambiguous physical public header")
        aliases[original] = alias
        i += 1
    return aliases


def include_directive(alias, groups):
    require(isinstance(groups, list) and len(groups) == 2, "missing CcInfo include directories")
    parts = alias.split("/")
    for dirs, template in zip(groups, ("#include <{}>", '#include "{}"')):
        require(isinstance(dirs, list) and dirs == sorted(set(dirs)), "invalid CcInfo include directories")
        for i in reversed(range(len(parts))):
            if ("/".join(parts[:i]) or ".") in dirs:
                return template.format("/".join(parts[i:]))
    raise ValueError("no exported include spelling for " + alias)


def values(argv, flag):
    require(argv.count(flag) == 1, "missing or duplicate " + flag)
    i = argv.index(flag) + 1
    return argv[i:next((j for j in range(i, len(argv)) if argv[j].startswith("--")), len(argv))]


def configured_header_graph(raw, rows, labels):
    selected = {k: row for k, row in rows.items() if row["label"] in labels}
    targets = {str(t["id"]): canonical(t["label"])
               for t in raw["targets"]}
    require(all(targets[str(a["targetId"])] in labels for a in raw["actions"] if a["mnemonic"] in (PP, PROCESS)),
            "DWYU compiler or processor action outside selected owners")
    actions = [a for a in raw["actions"] if a["mnemonic"] in (PP, PROCESS, ANALYZE)
               and targets[str(a["targetId"])] in labels]
    return RuntimeActionGraph(dict(raw, actions=actions), (PP, PROCESS, ANALYZE), selected, rule_classes={"cc_library"})


def verify_actions(graph, row, manifest, report, config):
    """Bind each declared primary header to Clang, the trace parser and this report."""
    cc = row["standalone_headers"]
    textual = cc.get("textual_files", [])
    aliases = {} if textual else header_aliases(cc, manifest)
    actions = graph.owners[identity(row["label"], row["checksum"])]
    require((aliases or textual) and max(len(aliases), len(textual)) <= MAX_HEADERS and
            len(actions) == 2 * len(aliases) + 1, "missing or extra header actions")
    by_output = {}
    ids = {}
    path_counts = {}
    for path in graph.artifacts.values():
        path_counts[path] = path_counts.get(path, 0) + 1
    for a in actions:
        descriptors = [graph.aspect_descriptors[str(i)] for i in a.get("aspectDescriptorIds", [])]
        parameters = descriptors[0].get("parameters", []) if descriptors else []
        require(tuple(d.get("name") for d in descriptors) == ASPECTS and not any(d.get("parameters") for d in descriptors[1:]) and
                len(parameters) == len(DWYU_PARAMETERS) and {p["key"]: p["value"] for p in parameters} == DWYU_PARAMETERS,
                "unexpected DWYU aspect or parameters")
        require(re.fullmatch("[0-9a-f]{64}", a["actionKey"]), "missing action key")
        for i in a["outputIds"]:
            f = graph.artifacts[str(i)]
            require(path_counts[f] == 1 and f not in by_output and f not in graph.tree_artifacts, "ambiguous header action output")
            by_output[f], ids[f] = a, str(i)
    for f in set(aliases) | set(aliases.values()) | {manifest["artifact"]}:
        require(path_counts.get(f) == 1 and f not in graph.tree_artifacts, "ambiguous or missing declared header File")
    analyzer = by_output[report]
    aa = analyzer_arguments(analyzer)
    tools = r"bazel-out/[^/]+/bin/bazel/rules/dwyu_"
    require(analyzer["mnemonic"] == ANALYZE and re.fullmatch(tools + "analyze_includes", aa[0]) and
            aa[0] in graph.inputs(analyzer) and analyzer_option(aa, "--owned_headers") == manifest["artifact"],
            "incorrect header analyzer")
    if textual:
        require(set(textual) == set(manifest["contents"]["required"]) == set(manifest["contents"]["public"].values())
                and not manifest["contents"]["private"] and not values(aa, "--preprocessed_public_files")
                and not values(aa, "--preprocessed_private_files"), "unexpected textual-only analyzer input")
        return {"reason": "textual-only; no modular public headers", "textual_files": textual, "report": report,
                "preprocess_actions": 0, "processor_actions": 0, "analyzer_action": analyzer["actionKey"],
                "owned_headers": manifest["artifact"], "cpp_qualified": False, "link_inputs_verified": False}
    ainputs = graph.input_ids(analyzer)
    pub, priv, primaries = set(), set(), set()
    headers = {}
    stem = report_stem(report)
    arch = {"cmake_match": "x86_64", "cmake_match_aarch64": "aarch64"}[config]
    compiler = f"bazel-out/buildbuddy_linux_{arch}-opt-exec/bin/external/llvm+/toolchain/stage1_linux_{arch}/bin/clang++"
    for proc in (a for a in actions if a["mnemonic"] == PROCESS):
        pa = analyzer_arguments(proc)
        source = analyzer_option(pa, "--source")
        alias = aliases[source]
        directive = include_directive(alias, row["standalone_headers"]["include_dirs"])
        pp, trace = (analyzer_option(pa, flag) for flag in ("--files", "--compiler_headers"))
        public, private = (analyzer_option(pa, flag) for flag in ("--output", "--private_output"))
        p = by_output[pp]
        require(source not in headers and pp not in primaries and p["mnemonic"] == PP and by_output[trace] is p and
                set(graph.files(p, "outputIds")) == {pp, trace} and len(p["outputIds"]) == 2 and
                pp.startswith(stem + ".dwyu_pp_pub_") and pp.endswith(".i") and trace == pp[:-2] + ".headers" and
                [public, private] == [pp[:-2].replace(".dwyu_pp_pub_", ".dwyu_psf_pub_") + suffix
                                     for suffix in ("_public.json", "_private.json")] and
                set(graph.files(proc, "outputIds")) == {public, private} and len(proc["outputIds"]) == 2 and public != private and
                {ids[pp], ids[trace]} <= graph.input_ids(proc) and {ids[public], ids[private]} <= ainputs,
                "missing or mismatched compiler/processor/analyzer File edge")
        command_json = analyzer_option(pa, "--compiler_command")
        command = json.loads(command_json)
        require(isinstance(command, list) and all(isinstance(s, str) for s in command) and
                command.count("-c") == command.count("-") == command.count("-o") == command.count(pp) == 1 and
                command[command.index("-c") + 1:command.index("-c") + 2] == ["-"] and command[command.index("-o") + 1:command.index("-o") + 2] == [pp] and
                not any(s.startswith(("-x", "@")) or s in ("-E", "-dI", "-header-include-file", "-fshow-skipped-includes", "-sys-header-deps") for s in command),
                "unexpected public-header compiler command")
        require(command.count("-target") == 1 and
                command[command.index("-target") + 1] in (arch + "-linux-gnu", arch + "-unknown-linux-gnu") and
                not any(arg.startswith(("--target", "-target=", "-arch=", "-Xarch", "--config=")) or
                        arg in ("-arch", "-triple", "-aux-triple", "-m32", "-mx32", "-m16", "-cc1", "--config")
                        for arg in command), "Clang target differs from the requested platform")
        toolchain = values(pa, "--toolchain_files")
        require(len(set(toolchain)) == len(toolchain) and compiler in toolchain and not set(toolchain).intersection(manifest["contents"]["public"]) and
                re.fullmatch(tools + "preprocessing", pa[0]) and
                pa == [pa[0], "--files", pp, "--mode", "compiler", "--source", source, "--included_header", alias,
                       "--compiler_headers", trace, "--compiler_command", command_json, "--toolchain_files", *toolchain,
                       "--output", public, "--private_output", private, "--owned_headers", manifest["artifact"]] and
                {source, alias, manifest["artifact"], pa[0]} <= graph.inputs(proc), "incorrect physical-header processor")
        pp_inputs = graph.inputs(p)
        require(all(f in pp_inputs for f in toolchain), "undeclared compiler toolchain input")
        argv = analyzer_arguments(p)
        replay = ["-E" if s == "-c" else s for s in command]
        require(argv[0] in ("/bin/bash", "/usr/bin/bash", "/bin/sh", "/usr/bin/sh") and
                argv[1:] == ["-c", SHELL, "", compiler, pp, trace, directive, "-x", "c++", *replay, "-dI",
                             "-fshow-skipped-includes", "-Xclang", "-header-include-file", "-Xclang", trace, "-Xclang", "-sys-header-deps"] and
                {source, alias, manifest["artifact"], compiler} <= pp_inputs, "Clang command or inputs differ from CcInfo")
        primaries.add(pp)
        pub.add(public)
        priv.add(private)
        headers[source] = {"included_header": alias, "directive": directive, "preprocessed": pp, "trace": trace,
                           "public_includes": public, "private_includes": private,
                           "compiler_action": p["actionKey"], "process_action": proc["actionKey"]}
    require(set(headers) == set(aliases) and pub.isdisjoint(priv) and sorted(pub) == sorted(values(aa, "--preprocessed_public_files")) and
            sorted(priv) == sorted(values(aa, "--preprocessed_private_files")), "incomplete header processor inputs to analyzer")
    contents = json.dumps(manifest["contents"], separators=(",", ":"), sort_keys=True).encode()
    return {"headers": headers, "public_files": row["standalone_headers"]["public"],
            "include_dirs": row["standalone_headers"]["include_dirs"], "owned_headers_sha256": hashlib.sha256(contents).hexdigest(),
            "analyzer_action": analyzer["actionKey"], "report": report,
            "cpp_qualified": False, "link_inputs_verified": False, "owned_headers": manifest["artifact"], "has_link_inputs": row["standalone_headers"]["has_link_inputs"]}


def successful_report(row, proof, ordinary, strict):
    """Accept the configured report only after read_strict matched its original named set."""
    s, o = row["strict"], row["ordinary"]
    if not (dwyu_request(strict.get("command")) == (ASPECTS[0], DWYU_PARAMETERS) and
            s.get("status") == "clean" and o.get("status") == "passed" and
            o.get("invocation_uuid") == ordinary.get("invocation_uuid") != strict.get("invocation_uuid") == s.get("invocation_uuid") and
            all(re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", r.get("invocation_uuid") or "")
                for r in (o, s)) and s.get("bep_file") and type(s.get("previous_report_removed")) is bool and
            s.get("failed_actions") == [] and s.get("findings") and not any(s["findings"].values())):
        return False
    for events, is_strict in ((o["completion"], False), (s["completions"], True)):
        checked = select_completions(events, is_strict, command=strict.get("command"), row=row)
        if len(checked) != 1 or checked != events or checked[0]["completed"].get("success") is not True:
            return False
    raw = s["original_json"].encode()
    report = json.loads(raw)
    original = s["report"]
    f = s["bep_file"]
    return (canonical(report["analyzed_target"]) == row["label"] and not any(report[field] for field in FIELDS) and
            not any(report.get(k) for k in ("unentered_owned_files", "unverified_compilation_deps", "unverified_compiler_inputs")) and
            hashlib.sha256(raw).hexdigest() == original["sha256"] and len(raw) == original["bytes"] and
            str(Path(*f.get("pathPrefix", []), f["name"])) == proof["report"] and
            bep_file_matches(f, original, local_path=Path(original["file"])))


def check_sources(workspace):
    registry = json.loads((workspace / "MODULE.bazel.lock").read_bytes())["registryFileHashes"]
    require(all(registry.get("https://bcr.bazel.build/modules/" + name) == digest
                for name, digest in REGISTRY_SHA256.items()), "Bazel registry source changed")
    for path, expected in SOURCE_SHA256.items():
        require(hashlib.sha256((workspace / path).read_bytes()).hexdigest() == expected,
                "DWYU public-header verifier needs review for changed " + path)

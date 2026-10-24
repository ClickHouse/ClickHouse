"""Check original protobuf-generated C++ compilations and their DWYU reports."""

from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re

from audit_full_dwyu import (
    RuntimeActionGraph, bep_file_matches, canonical, census_canonical, identity,
    ordinary_reachability, query_labels, sha_file, _RUNTIME_PP_SHELL,
)


PROTO_KINDS = ("proto_library", "cc_proto_library")
ORIGINAL_ACTIONS = ("GenProto", "GenProtoDescriptorSet", "CppCompile",
                    "CppCompileHeader", "CppModuleCompile", "CppLink", "CppArchive")
REPORT_SUFFIX = ".proto_dwyu_report.json"
ROUTING_SUFFIX = ".proto_dwyu_routing.json"
PROTO_ASPECT = "--aspects=//bazel/rules:proto_dwyu.bzl%proto_dwyu"
NATIVE_ACTIONS = ("ClickHouseNativeArchive", "ClickHouseNativeLink")
ARCHIVE_ACTIONS = ("CppArchive", "CppLink")


def output_path(name):
    """Accept only artifacts under this execution root, never a file outside it."""
    if (not isinstance(name, str) or not name or Path(name).is_absolute()
            or ".." in Path(name).parts or not name.startswith("bazel-out/")):
        raise ValueError("invalid generated protobuf output: " + repr(name))
    return name


def read_routing_record(bep, execroot, produced, key, name, record):
    routing_name = name[:-len(REPORT_SUFFIX)] + ROUTING_SUFFIX
    routing_key, action = produced[routing_name]
    routing, evidence = bep.read_report(execroot, key, routing_name)
    expected = {
        "schema": 1,
        "record_kind": "PROTO_DWYU_ROUTING",
        "routing_file": routing_name,
        "label": record["label"],
        "configured_owner": record["configured_owner"],
        "kind": record["kind"],
        "status": record["status"],
        "problems": [problem["reason"] for problem in record["problems"]],
        "generated_sources": record["generated_sources"],
        "generated_headers": record["generated_headers"],
        "compile_actions": [{"src": action["src"], "object": action["object"]}
                            for action in record["compile_actions"]],
        "direct_proto_deps": [{"label": dep["label"], "configured_owner": dep["configured_owner"]}
                              for dep in record["direct_proto_deps"]],
    }
    if (routing_key != key or action["mnemonic"] != "FileWrite"
            or action.get("fileContents") is None or json.loads(action["fileContents"]) != routing
            or routing != expected):
        raise ValueError("proto routing differs from its original configured FileWrite: " + routing_name)
    return evidence


def environment(action):
    result = {}
    for item in action.get("environmentVariables", []):
        key, value = item["key"], item["value"]
        if not isinstance(key, str) or not isinstance(value, str) or key in result:
            raise ValueError("duplicate or invalid action environment")
        result[key] = value
    return result


class ProtoBep:
    """Only named files under successful requested roots can qualify cached outputs."""

    def __init__(self, path, roots, rows, group=None):
        self.path = sha_file(path)
        self.actions = defaultdict(list)
        self.named = {}
        self.successful_roots = set()
        starts, sets, remaining = [], {}, []
        requested = {census_canonical(label) for label in roots}
        with path.open() as stream:
            for line in stream:
                event = json.loads(line)
                if "started" in event:
                    starts.append(event["started"].get("uuid"))
                completed = event.get("id", {}).get("actionCompleted")
                if completed:
                    key = identity(completed["label"], completed.get("configuration", {}).get("id", ""))
                    name = completed["primaryOutput"]
                    action = event.get("action", event.get("actionExecuted", {}))
                    if name.startswith("bazel-out/"):
                        self.actions[key, output_path(name)].append(action)
                if "namedSet" in event.get("id", {}):
                    ident = event["id"]["namedSet"]["id"]
                    if ident in sets and sets[ident] != event["namedSetOfFiles"]:
                        raise ValueError("inconsistent proto BEP named set")
                    sets[ident] = event["namedSetOfFiles"]
                target = event.get("id", {}).get("targetCompleted")
                if not target or canonical(target.get("label", "")) not in requested:
                    continue
                key = identity(target["label"], target.get("configuration", {}).get("id", ""))
                if key not in rows:
                    raise ValueError("proto BEP root outside original cquery: " + key)
                complete = event.get("completed", {})
                groups = complete.get("outputGroup", [])
                if group is None and target.get("aspect"):
                    continue
                chosen = [item for item in groups if group is None or item.get("name") == group]
                if complete.get("success") is True and chosen:
                    self.successful_roots.add(key)
                    remaining.extend(ref["id"] for item in chosen for ref in item.get("fileSets", []))
        if len(starts) != 1 or not starts[0]:
            raise ValueError("proto BEP must contain exactly one original invocation")
        self.invocation_uuid = starts[0]
        seen = set()
        while remaining:
            ident = remaining.pop()
            if ident in seen:
                continue
            seen.add(ident)
            entry = sets[ident]
            remaining.extend(ref["id"] for ref in entry.get("fileSets", []))
            for item in entry.get("files", []):
                name = str(Path(*item.get("pathPrefix", [])) / item["name"])
                # An ordinary target can have source files in DefaultInfo.
                # Such a source can never qualify a generated C++ artifact.
                if not name.startswith("bazel-out/"):
                    continue
                output_path(name)
                if name in self.named and self.named[name] != item:
                    raise ValueError("inconsistent protobuf BEP artifact: " + name)
                self.named[name] = item

    def failed(self, key, output):
        return any(action.get("success") is not True for action in self.actions.get((key, output), []))

    def succeeded(self, key, output, mnemonic):
        return any(action.get("success") is True and action.get("type") == mnemonic
                   for action in self.actions.get((key, output), [])) and not self.failed(key, output)

    def output_evidence(self, key, name, mnemonic):
        """Only the requested roots' successful ordinary BEP outputs can prove cache hits."""
        if self.failed(key, name):
            raise ValueError("original " + mnemonic + " action failed: " + name)
        if self.succeeded(key, name, mnemonic):
            return {"kind": "original_action", "bep": self.path, "invocation_uuid": self.invocation_uuid,
                    "owner": key, "mnemonic": mnemonic, "output": name}
        ref = self.named.get(name)
        if not ref:
            return None
        digest, size = ref.get("digest"), ref.get("length")
        if digest is None and size is None:
            match = re.fullmatch(
                r"bytestream://[A-Za-z0-9][A-Za-z0-9.-]*(?::[0-9]+)?"
                r"(?:/[A-Za-z0-9][A-Za-z0-9._-]*)*/blobs/([0-9a-f]{64})/(0|[1-9][0-9]*)",
                ref.get("uri", ""),
            )
            if match:
                digest, size = match[1], match[2]
        if (not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
                or type(size) not in (str, int) or not re.fullmatch(r"(0|[1-9][0-9]*)", str(size))
                or not bep_file_matches(ref, {"sha256": digest, "bytes": int(size)})):
            raise ValueError("successful requested output has no valid digest: " + name)
        return {"kind": "original_requested_output", "bep": self.path, "invocation_uuid": self.invocation_uuid,
                "owner": key, "mnemonic": mnemonic, "output": name,
                "sha256": digest, "bytes": int(size), "bep_file": ref}

    def read_file(self, execroot, key, name):
        path = execroot / output_path(name)
        if self.failed(key, name):
            raise ValueError("original preprocessing or report action failed: " + name)
        if not path.is_file():
            raise ValueError("protobuf report was not produced: " + name)
        original = sha_file(path)
        references = [self.named.get(name)]
        references += [action.get("primaryOutput") for action in self.actions.get((key, name), [])
                       if action.get("success") is True]
        reference = next((item for item in references if bep_file_matches(item, original)), None)
        if reference is None:
            raise ValueError("protobuf report has no matching successful original BEP digest: " + name)
        return path.read_bytes(), {"report": original, "bep_file": reference}

    def read_report(self, execroot, key, name):
        raw, evidence = self.read_file(execroot, key, name)
        return json.loads(raw), evidence


def cc_proto_descriptor(graph, action):
    """Inspect original aquery aspect IDs; CcInfo from another aspect is insufficient."""
    descriptors = []
    for ident in action.get("aspectDescriptorIds", []):
        descriptor = graph.aspect_descriptors.get(str(ident))
        if descriptor is None:
            raise ValueError("missing original protobuf aspect descriptor: " + str(ident))
        descriptors.append(descriptor)
    valid = [descriptor for descriptor in descriptors
             if re.fullmatch(r"@@protobuf[+~][^/]*//bazel/private/oss:cc_proto_library[.]bzl%cc_proto_aspect",
                             descriptor.get("name", ""))]
    if len(valid) != 1:
        raise ValueError("compile did not come from the pinned protobuf cc_proto_aspect")
    return valid[0]


def configured_descendants(rows, selected):
    visited, pending = set(), list(selected)
    while pending:
        key = pending.pop()
        if key in visited:
            continue
        visited.add(key)
        for dep in rows[key]["configured_inputs"]:
            checksum = dep.get("configurationChecksum")
            if not checksum:
                continue
            child = identity(dep["label"], checksum)
            if child in rows and not rows[child]["is_tool"]:
                pending.append(child)
    return visited


def graph_source(graph, action):
    cmd = action.get("arguments", [])
    if not cmd or action.get("paramFiles"):
        raise ValueError("unexpanded protobuf compile arguments")
    sources = [cmd[i + 1] for i, arg in enumerate(cmd[:-1]) if arg == "-c"]
    outputs = [cmd[i + 1] for i, arg in enumerate(cmd[:-1]) if arg == "-o"]
    if (len(sources) != 1 or len(outputs) != 1 or outputs[0] not in graph.files(action, "outputIds")
            or sources[0] not in graph.inputs(action) or sources[0] in graph.tree_artifacts
            or outputs[0] in graph.tree_artifacts):
        raise ValueError("protobuf compile lacks one original source and object")
    if Path(sources[0]).suffix not in (".cc", ".cpp", ".cxx", ".c++", ".C"):
        raise ValueError("protobuf CppCompile has no C++ translation unit")
    return sources[0], output_path(outputs[0])


def compile_inputs(key, original, reports, compiled):
    """Match the declared and current aquery inputs for each original CppCompile."""
    current = {}
    for action in reports.owners.get(key, []):
        if action["mnemonic"] == "CppCompile":
            unit = graph_source(reports, action)
            if unit in current:
                raise ValueError("duplicate current protobuf CppCompile: " + unit[1])
            current[unit] = action

    inputs = {}
    for cpp in compiled:
        unit = graph_source(original, cpp)
        action = current.pop(unit, None)
        if action is None:
            raise ValueError("missing current protobuf CppCompile: " + unit[1])
        aspect = cc_proto_descriptor(original, cpp)
        current_aspect = cc_proto_descriptor(reports, action)
        if (not isinstance(cpp.get("actionKey"), str) or not cpp["actionKey"]
                or cpp["actionKey"] != action.get("actionKey")
                or bool(cpp.get("discoversInputs")) != bool(action.get("discoversInputs"))
                or any(aspect.get(k) != current_aspect.get(k) for k in ("name", "parameters"))
                or cpp["arguments"] != action["arguments"] or environment(cpp) != environment(action)
                or set(original.files(cpp, "outputIds")) != set(reports.files(action, "outputIds"))):
            raise ValueError("declared and current protobuf CppCompile differ: " + unit[1])
        inputs[unit[1]] = original.inputs(cpp) | reports.inputs(action)
    if current:
        raise ValueError("unexpected current protobuf CppCompile for " + key)
    return inputs


def exact_recorded_compile(graph, cpp, observed, inputs):
    src, obj = graph_source(graph, cpp)
    if (observed.get("src") != src or observed.get("object") != obj
            or observed.get("argv") != cpp.get("arguments")
            or observed.get("environment") != environment(cpp)
            or not isinstance(observed.get("inputs"), list)
            or len(observed["inputs"]) != len(inputs) or set(observed["inputs"]) != inputs):
        fields = (
            ("src", observed.get("src"), src),
            ("object", observed.get("object"), obj),
            ("argv", observed.get("argv"), cpp.get("arguments")),
            ("environment", observed.get("environment"), environment(cpp)),
            ("inputs", sorted(observed["inputs"]) if isinstance(observed.get("inputs"), list)
             else observed.get("inputs"), sorted(inputs)),
        )
        mismatches = {}
        for name, recorded, expected in fields:
            if recorded == expected:
                continue
            info = {}
            for key, value in (("recorded", recorded), ("expected", expected)):
                raw = json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()
                info[key] = {"type": type(value).__name__, "sha256": hashlib.sha256(raw).hexdigest(),
                             "count": len(value) if isinstance(value, (str, list, dict)) else None}
            if name in ("src", "object", "argv", "inputs"):
                left = recorded if isinstance(recorded, list) else [recorded]
                right = expected if isinstance(expected, list) else [expected]
                for index in range(max(len(left), len(right))):
                    a = left[index] if index < len(left) else None
                    b = right[index] if index < len(right) else None
                    if a == b:
                        continue
                    info["first_difference_index"] = index
                    for key, value in (("recorded", a), ("expected", b)):
                        if (isinstance(value, str) and len(value) <= 256
                                and re.fullmatch(
                                    r"(?:(?:bazel-out|external|src)/[A-Za-z0-9_/.+~-]+"
                                    r"\.(?:c|cc|cpp|cxx|h|hh|hpp|o|obj|d|a|bc)|"
                                    r"-(?:O[0123szg]|g[0-3]?|f(?:PIC|PIE|no-exceptions|no-rtti)))", value)
                                and ".." not in Path(value).parts):
                            info["first_" + key] = value
                    break
            mismatches[name] = info
        raise ValueError("protobuf observer differs from original CppCompile: " +
                         json.dumps(mismatches, sort_keys=True, separators=(",", ":")))
    return src, obj


def recorded_output(actions, graph):
    result = {}
    for key, owned in actions.items():
        for action in owned:
            for name in graph.files(action, "outputIds"):
                if name in result:
                    raise ValueError("multiple original actions produce " + name)
                result[name] = (key, action)
    return result


def native_link_inputs(seeds, graph, native, coverage):
    """Prove declared files or objects in the actual ClickHouse native link graph."""
    candidates = {name: {"source": name, "native_archive": None} for name in seeds}
    for key, actions in graph.owners.items():
        for action in actions:
            if action["mnemonic"] != "ClickHouseNativeArchive":
                continue
            entered = seeds & graph.inputs(action)
            if not entered:
                continue
            for output in graph.files(action, "outputIds"):
                if native.failed(key, output):
                    raise ValueError("original ClickHouseNativeArchive failed: " + output)
                for name in sorted(entered):
                    if output in candidates and candidates[output]["source"] != name:
                        # Multiple requested objects can go into the same archive.
                        # Each caller supplies files for one original owner only.
                        continue
                    candidates[output] = {
                        "source": name,
                        "native_archive": {"owner": key, "mnemonic": action["mnemonic"],
                                           "input": name, "output": output},
                    }
    proofs = []
    for key, actions in graph.owners.items():
        for action in actions:
            if action["mnemonic"] != "ClickHouseNativeLink":
                continue
            entered = set(candidates) & graph.inputs(action)
            if not entered:
                continue
            for output in graph.files(action, "outputIds"):
                # Explicit failed archives or links win even if a CppCompile action
                # succeeded or another ordinary root published an older output.
                evidence = native.output_evidence(key, output, action["mnemonic"])
                if key not in coverage["reachable"] or evidence is None:
                    continue
                for name in sorted(entered):
                    repacked = candidates[name]["native_archive"]
                    if repacked and repacked["owner"] not in coverage["reachable"]:
                        continue
                    proofs.append({
                        **candidates[name],
                        "native_link": {"owner": key, "mnemonic": action["mnemonic"], "input": name,
                                        "output": output, "ordinary": evidence},
                    })
    return proofs


def original_compile_reached(key, cpp, graph, native, coverage, links, final_links):
    """Require original C++ or the exact object in a successfully requested output."""
    _, obj = graph_source(graph, cpp)
    if native.failed(key, obj):
        raise ValueError("original CppCompile failed: " + obj)
    archives = {}
    for link in links:
        if obj in graph.inputs(link):
            for archive in graph.files(link, "outputIds"):
                if native.failed(key, archive):
                    raise ValueError("original protobuf archive failed: " + archive)
                archives[archive] = link["mnemonic"]
    native_proofs = native_link_inputs({obj} | set(archives), final_links, native, coverage)
    if key not in coverage["reachable"]:
        raise ValueError("no successful original root reaches protobuf C++ owner")
    if native.succeeded(key, obj, "CppCompile"):
        return {"kind": "original_cpp_action", "object": obj,
                "ordinary": native.output_evidence(key, obj, "CppCompile")}
    for archive, mnemonic in archives.items():
        evidence = native.output_evidence(key, archive, mnemonic)
        if evidence:
            return {"kind": ("original_proto_link" if evidence["kind"] == "original_action"
                             else "original_requested_proto_archive"),
                    "object": obj, "archive": archive, "ordinary": evidence}
    if native_proofs:
        return {"kind": "original_completed_native_link_input", "object": obj,
                "linkage": native_proofs[0]}
    raise ValueError("CppCompile has no successful original action or link-input evidence: " + obj)


def preprocess_options(argv, outputs, src, obj):
    """Remove only original compiler output options; preserve every compile flag."""
    result, i = [], 1
    while i < len(argv):
        arg = argv[i]
        if arg in ("-c", "-o", "-MF", "-MT", "-MQ"):
            if i + 1 >= len(argv):
                raise ValueError("protobuf compile option missing operand: " + arg)
            value = argv[i + 1]
            if ((arg == "-c" and value != src) or (arg == "-o" and value != obj)
                    or (arg == "-MF" and value not in outputs)
                    or (arg in ("-MT", "-MQ") and value != obj)):
                raise ValueError("protobuf compile operand does not match original output: " + arg)
            i += 2
        elif arg in ("-MD", "-MMD", "-MP"):
            i += 1
        elif (arg.startswith(("-M", "@", "-c=")) or arg.startswith("-o")
              or arg in ("-E", "-S", "-emit-llvm")):
            raise ValueError("unsupported protobuf compiler option: " + arg)
        else:
            result.append(arg)
            i += 1
    return result


def expanded_args(action):
    argv = action.get("arguments", [])
    params = {file["execPath"]: file.get("arguments", []) for file in action.get("paramFiles", [])}
    result, used = [], set()
    for arg in argv:
        if arg.startswith("@"):
            path = arg[1:]
            if path not in params or path in used:
                raise ValueError("unexpanded or repeated protobuf analyzer param file")
            used.add(path)
            result.extend(params[path])
        else:
            result.append(arg)
    if used != set(params):
        raise ValueError("unmatched protobuf analyzer param files")
    return result


def option(argv, name):
    indices = [i for i, word in enumerate(argv) if word == name]
    if len(indices) != 1 or indices[0] + 1 >= len(argv):
        raise ValueError("missing or duplicated analyzer option: " + name)
    return argv[indices[0] + 1]


def bind_preprocessor(key, cpp, observed, record, original, reports, outputs, original_inputs):
    """Bind the report action to the original compiler, source and compiler options."""
    src, obj = graph_source(original, cpp)
    pair = observed.get("pp_report")
    if (not isinstance(pair, dict) or set(pair) != {"public", "private"}
            or pair["public"] == pair["private"]):
        raise ValueError("no original public/private protobuf preprocessing pair")
    pub, priv = output_path(pair["public"]), output_path(pair["private"])
    process_key, process = outputs[pub]
    if (process_key != key or outputs[priv] != (process_key, process)
            or process["mnemonic"] != "DwyuProcessSourceFile"
            or set(reports.files(process, "outputIds")) != {pub, priv}):
        raise ValueError("protobuf reports do not have a single original processing action")
    cmd = expanded_args(process)
    if (option(cmd, "--mode") != "compiler" or option(cmd, "--source") != src
            or option(cmd, "--output") != pub or option(cmd, "--private_output") != priv
            or option(cmd, "--owned_headers") != record["owned_headers"]
            or json.loads(option(cmd, "--compiler_command")) != cpp["arguments"][1:]):
        raise ValueError("protobuf source processor did not use the original compile command")
    trace, pp = option(cmd, "--compiler_headers"), option(cmd, "--files")
    pp_key, preprocess = outputs[pp]
    if (pp_key != key or outputs[trace] != (pp_key, preprocess)
            or preprocess["mnemonic"] != "DwyuCompilerPreprocess"
            or set(reports.files(preprocess, "outputIds")) != {pp, trace}):
        raise ValueError("protobuf report lacks a bound original compiler preprocessing action")
    argv = expanded_args(preprocess)
    options = preprocess_options(cpp["arguments"], original.files(cpp, "outputIds"), src, obj)
    expected = [cpp["arguments"][0], pp, trace] + options + [
        "-o", pp, "-E", src, "-dI", "-fshow-skipped-includes",
        "-Xclang", "-header-include-file", "-Xclang", trace, "-Xclang", "-sys-header-deps",
    ]
    if (len(argv) < 4 or argv[0] not in ("/bin/bash", "/usr/bin/bash", "/bin/sh", "/usr/bin/sh")
            or argv[1:4] != ["-c", _RUNTIME_PP_SHELL, ""] or argv[4:] != expected
            or environment(preprocess) != environment(cpp)):
        raise ValueError("protobuf preprocessor changed compiler options or environment: " + obj)
    pp_inputs = reports.inputs(preprocess)
    process_inputs = reports.inputs(process)
    if (not original_inputs <= pp_inputs or record["owned_headers"] not in pp_inputs
            or not original_inputs | {pp, trace, record["owned_headers"]} <= process_inputs):
        raise ValueError("protobuf preprocessing dropped original compiler inputs")
    return {"public": pub, "private": priv, "preprocess_output": pp, "header_trace": trace}


def read_header_uses(key, pair, record, headers, bep, execroot, allowed):
    entered, missing, used, evidence = set(), {"public": [], "private": []}, set(), []
    public = headers["public"]
    private = headers["private"]
    for visibility in ("public", "private"):
        records, proof = bep.read_report(execroot, key, pair[visibility])
        evidence.append(proof)
        if not isinstance(records, list):
            raise ValueError("invalid compiler preprocessing JSON")
        seen = set()
        owned = set(public.values()) if visibility == "public" else set(private.values())
        for item in records:
            if (not isinstance(item, dict) or set(item) != {"file", "entered", "resolved_includes"}
                    or item["file"] not in owned or item["file"] in seen
                    or type(item["entered"]) is not bool or not isinstance(item["resolved_includes"], list)):
                raise ValueError("invalid or non-owned file in protobuf preprocessing")
            seen.add(item["file"])
            if item["entered"]:
                entered.add(item["file"])
            for include in item["resolved_includes"]:
                if (not isinstance(include, dict) or not isinstance(include.get("include"), str)
                        or not isinstance(include.get("file"), str)
                        or set(include) - {"include", "file", "unsupported_search_flag"}):
                    raise ValueError("invalid resolved protobuf include")
                resolved = include["file"]
                if include.get("unsupported_search_flag"):
                    missing[visibility].append({"from": item["file"], **include})
                elif resolved in public or resolved in private:
                    continue
                elif resolved in allowed:
                    used.update(allowed[resolved])
                else:
                    missing[visibility].append({"from": item["file"], **include})
    return entered, missing, used, evidence


def validate_owned_headers(record, headers):
    generated = set(record["generated_headers"])
    sources = set(record["generated_sources"])
    if (not isinstance(headers, dict) or not isinstance(headers.get("public"), dict)
            or not isinstance(headers.get("private"), dict)
            or not isinstance(headers.get("required"), list)
            or set(headers["public"].values()) != generated
            or not generated <= set(headers["public"])
            or not set(record["direct_headers"]) <= set(headers["public"])
            or headers["private"] != {src: src for src in sources}
            or set(headers["required"]) != generated | sources
            or headers.get("unchecked_link_inputs") or headers.get("raw_assembly")
            or headers.get("raw_link_candidates")):
        raise ValueError("proto owned headers differ from GenProto and direct CcInfo")


def original_generation(key, record, graph, native, coverage):
    actions = graph.owners.get(key, [])
    generation = [a for a in actions if a["mnemonic"] == "GenProto"]
    if len(record["generation_actions"]) != len(generation):
        raise ValueError("protobuf observer GenProto count differs from original")
    generated = set()
    recorded = {tuple(sorted(item["outputs"])): item for item in record["generation_actions"]}
    if len(recorded) != len(generation):
        raise ValueError("duplicate observed protobuf generation output")
    for action in generation:
        cc_proto_descriptor(graph, action)
        out = set(graph.files(action, "outputIds"))
        observed = recorded.get(tuple(sorted(out)), {})
        if (observed.get("mnemonic") != "GenProto" or observed.get("argv") != action.get("arguments")
                or observed.get("environment") != environment(action)
                or set(observed.get("inputs", [])) != graph.inputs(action)
                or set(observed.get("outputs", [])) != out):
            raise ValueError("protobuf observer differs from original GenProto action")
        if any(native.failed(key, name) for name in out) or key not in coverage["reachable"]:
            raise ValueError("no successful original generation for " + key)
        if generated & out:
            raise ValueError("overlapping original protobuf generation outputs")
        generated.update(out)
    if generated != set(record["generated_sources"]) | set(record["generated_headers"]):
        raise ValueError("protobuf generated sources/headers do not match original GenProto outputs")
    return generated


def wire_fields(raw):
    """Read the fields needed from a real protoc FileDescriptorSet."""
    def varint(pos):
        result = 0
        for shift in range(0, 70, 7):
            if pos >= len(raw):
                raise ValueError("truncated protobuf descriptor varint")
            byte = raw[pos]
            pos += 1
            result |= (byte & 127) << shift
            if byte < 128:
                return result, pos
        raise ValueError("invalid protobuf descriptor varint")
    pos = 0
    while pos < len(raw):
        tag, pos = varint(pos)
        field, wire = tag >> 3, tag & 7
        if not field:
            raise ValueError("zero field in protobuf descriptor")
        if wire == 0:
            value, pos = varint(pos)
        else:
            if wire == 2:
                size, pos = varint(pos)
            elif wire == 1:
                size = 8
            elif wire == 5:
                size = 4
            else:
                raise ValueError("unsupported wire type in protobuf descriptor")
            value = raw[pos:pos + size]
            pos += size
            if len(value) != size:
                raise ValueError("truncated protobuf descriptor field")
        yield field, wire, value


def descriptor_imports(raw, expected_sources):
    files = {}
    for field, wire, value in wire_fields(raw):
        if field != 1 or wire != 2:
            raise ValueError("unknown FileDescriptorSet field")
        names, dependencies = [], []
        for number, value_wire, data in wire_fields(value):
            if number in (1, 3):  # FileDescriptorProto.name / dependency
                if value_wire != 2:
                    raise ValueError("invalid FileDescriptorProto import field")
                text = data.decode("utf-8")
                if not text or text.startswith("/") or ".." in Path(text).parts:
                    raise ValueError("invalid protobuf import name")
                (names if number == 1 else dependencies).append(text)
        if len(names) != 1 or names[0] in files:
            raise ValueError("duplicate or missing descriptor source")
        files[names[0]] = dependencies
    if set(files) != set(expected_sources):
        raise ValueError("original descriptor source names differ from ProtoInfo")
    return files


def declared_dependencies(row, attr):
    info = row["dependency_attributes"].get(attr, {})
    if "selectorList" in info:
        raise ValueError("protobuf dependency selection was not configured")
    return {census_canonical(label) for label in info.get("stringListValue", [])}


def owner_report(owner, metadata, rows):
    name = output_path(owner["configured_owner"])
    if name not in metadata:
        raise ValueError("protobuf dependency has no configured report: " + name)
    key, report, _ = metadata[name]
    if canonical(owner["label"]) != rows[key]["label"] or report["configured_owner"] != name:
        raise ValueError("protobuf dependency report has another label/configuration")
    if set(owner["direct_headers"]) != set(report["direct_headers"]):
        raise ValueError("protobuf dependency headers differ from its configured owner")
    return key


def dependency_owners(row, record, metadata, rows, runtime_graph, final_links, native, coverage):
    allowed = defaultdict(set)
    deps = record["direct_proto_deps"]
    observed = {canonical(dep["label"]) for dep in deps}
    if len(observed) != len(deps) or observed != declared_dependencies(row, "deps"):
        raise ValueError("protobuf direct deps differ from original configured rule")
    configured = {
        identity(dep["label"], dep["configurationChecksum"])
        for dep in row["configured_inputs"] if dep.get("configurationChecksum")
    }
    for dep in deps:
        child = owner_report(dep, metadata, rows)
        if child not in configured:
            raise ValueError("proto dependency came from another original configuration")
        for header in dep["direct_headers"]:
            allowed[header].add(child)
    runtime = record["runtime"]
    if (not isinstance(runtime, dict) or not runtime.get("outputs") or not runtime.get("direct_headers")
            or not isinstance(runtime.get("headers"), list)
            or not set(runtime["direct_headers"]) <= set(runtime["headers"]) or runtime_graph is None):
        raise ValueError("missing target protobuf runtime")
    runtime_files = {output_path(name) for name in runtime["outputs"]}
    output_path(runtime["configured_owner"])
    if runtime["configured_owner"] not in runtime_files:
        raise ValueError("runtime identity does not come from its actual outputs")
    candidates = {}
    for key, runtime_row in rows.items():
        if (canonical(runtime["label"]) != runtime_row["label"] or runtime_row["is_tool"]
                or key not in coverage["reachable"]):
            continue
        actions = runtime_graph.owners.get(key, [])
        produced = {out for action in actions for out in runtime_graph.files(action, "outputIds")}
        archives = {out for out in runtime_files & produced if out.endswith((".a", ".lo", ".lib"))}
        if runtime["configured_owner"] in produced and archives:
            candidates[key] = sorted(archives)
    if len(candidates) != 1:
        raise ValueError("target protobuf runtime archive has no unique non-tool configured owner")
    owner, archives = next(iter(candidates.items()))
    for header in runtime["direct_headers"]:
        allowed[header].add(owner)
    # The selected toolchain's CcInfo and its matching configured archive
    # producer prove header ownership. They do not prove runtime execution.
    ownership = {"status": "declared_target_archive", "owner": owner, "declared_archives": archives,
                 "configured_owner": runtime["configured_owner"],
                 "direct_headers": sorted(runtime["direct_headers"]),
                 "cpp_qualified": False, "native_qualified": False}
    for archive in archives:
        if native.failed(owner, archive):
            raise ValueError("original target runtime archive failed: " + archive)
    proofs = native_link_inputs(set(archives), final_links, native, coverage)
    runtime_native = {"status": "unproved", "native_qualified": False}
    if proofs:
        runtime_native = {"status": "matched_original_native_link", "native_qualified": True,
                          "owner": owner, "runtime_archive": proofs[0]["source"], "linkage": proofs[0]}
    return allowed, ownership, runtime_native


def schema_evidence(key, row, record, graph, native, coverage, report_bep, execroot):
    name = output_path(record["direct_descriptor_set"])
    actions = [a for a in graph.owners.get(key, []) if name in graph.files(a, "outputIds")]
    if len(actions) != 1:
        raise ValueError("no unique original action for ProtoInfo descriptor")
    action = actions[0]
    if action.get("aspectDescriptorIds") or native.failed(key, name) or key not in coverage["reachable"]:
        raise ValueError("protobuf descriptor not reached by an original successful rule")
    sources = record["proto_sources"]
    if sources:
        if action["mnemonic"] != "GenProtoDescriptorSet" or not set(sources) <= graph.inputs(action):
            raise ValueError("descriptor did not compile the actual ProtoInfo sources")
    elif action["mnemonic"] != "FileWrite":
        raise ValueError("sourceless protobuf descriptor did not come from its rule")
    raw, evidence = report_bep.read_file(execroot, key, name)
    imports = descriptor_imports(raw, record["import_paths"])
    retained, unavailable = {}, []
    imported = {item for items in imports.values() for item in items}
    for attr, list_name in (("deps", "direct_proto_deps"), ("exports", "exports"), ("option_deps", "option_deps")):
        declarations = declared_dependencies(row, attr)
        owners = record[list_name]
        if declarations != {canonical(dep["label"]) for dep in owners}:
            raise ValueError("ProtoInfo " + attr + " differs from configured rule")
        for dep in owners:
            uses = sorted(imported & set(dep["import_paths"]))
            label = canonical(dep["label"])
            if uses:
                retained.setdefault(label, {})[attr] = uses
            elif label not in retained:
                unavailable.append({"attribute": attr, "label": label,
                                    "reason": "descriptor has no proved import; no C++ dep change recommended"})
    return {"descriptor": evidence, "imports": imports, "retained": retained,
            "not_proved": unavailable, "implementation_deps_recommendations": []}


def audit_proto(audit, roots, rows, selected, native_bep, execroot):
    result = {"scope_complete": False, "compiled_owners": {}, "forwarders": {},
              "unqualified": {}, "unqualified_reports": {},
              "selected_original_cc_proto": sorted(selected)}
    proto_labels = sorted({row["label"] for row in selected.values()})
    root_scope = "deps(" + query_labels(roots) + ")"
    scope = ("kind('^proto_library( rule)?$', "
             "deps(kind('^cc_proto_library( rule)?$', " + root_scope + ")))")
    proto_scope = "deps(" + query_labels(proto_labels) + ")"
    include = ("--include_aspects", "--include_artifacts", "--include_commandline", "--include_param_files")
    original_path = audit.query("proto-original-actions", "aquery",
                                "mnemonic('^(GenProto|GenProtoDescriptorSet|CppCompile|CppCompileHeader|"
                                "CppModuleCompile|CppLink|CppArchive|FileWrite)$', " + scope + ")",
                                ("--include_pruned_inputs",) + include)
    report_path = audit.query("proto-report-actions", "aquery",
                              "mnemonic('^(CppCompile|FileWrite|DwyuCompilerPreprocess|DwyuProcessSourceFile)$', "
                              "kind('^(proto_library|cc_proto_library)( rule)?$', " + proto_scope + "))",
                              (PROTO_ASPECT, "--output_groups=proto_dwyu", "--include_file_write_contents",
                               "--noinclude_pruned_inputs") + include)
    link_path = audit.query("proto-original-final-links", "aquery",
                            "mnemonic('^(ClickHouseNativeArchive|ClickHouseNativeLink)$', " + root_scope + ")",
                            ("--include_artifacts", "--include_aspects"))
    original = RuntimeActionGraph(original_path, ORIGINAL_ACTIONS + ("FileWrite",), rows, ("proto_library",))
    reports = RuntimeActionGraph(report_path, ("CppCompile", "FileWrite", "DwyuCompilerPreprocess",
                                              "DwyuProcessSourceFile"), rows, PROTO_KINDS)
    if any(a.get("discoversInputs") for actions in reports.owners.values() for a in actions
           if a["mnemonic"] != "CppCompile"):
        raise ValueError("protobuf report action discovers inputs")
    links = RuntimeActionGraph(link_path, NATIVE_ACTIONS, rows, None)
    result["queries"] = {"ordinary": sha_file(original_path), "reports": sha_file(report_path),
                         "links": sha_file(link_path)}
    result["original_bep"] = sha_file(native_bep)
    native = ProtoBep(native_bep, roots, rows)
    coverage = ordinary_reachability(native_bep, rows, roots)
    expected = configured_descendants(rows, selected)
    bep = audit.out / "proto-strict.bep.json"
    run = audit.command("proto-strict", "build", [
        "--keep_going", PROTO_ASPECT, "--output_groups=proto_dwyu",
        "--remote_download_outputs=toplevel", "--build_event_publish_all_actions",
        "--build_event_json_file=" + str(bep),
    ] + proto_labels)
    result["build"] = run
    pp_bep = ProtoBep(bep, proto_labels, rows, "proto_dwyu")
    result["report_bep"] = pp_bep.path
    produced = recorded_output(reports.owners, reports)
    metadata, routing_evidence = {}, {}
    for name, (key, action) in produced.items():
        if not name.endswith(REPORT_SUFFIX):
            continue
        if key not in expected or rows[key]["is_tool"]:
            result["unqualified"][key] = "observer outside selected original non-tool proto closure"
            continue
        try:
            record, evidence = pp_bep.read_report(execroot, key, name)
            if (record.get("schema") != 1 or canonical(record.get("label", "")) != rows[key]["label"]
                    or record.get("configured_owner") != name or record.get("kind") != rows[key]["rule_class"]
                    or action["mnemonic"] != "FileWrite" or action.get("fileContents") is None
                    or json.loads(action["fileContents"]) != record):
                raise ValueError("proto observer metadata differs from configured FileWrite")
            routing_evidence[name] = read_routing_record(pp_bep, execroot, produced, key, name, record)
            metadata[name] = (key, record, evidence)
        except (KeyError, TypeError, ValueError, OSError) as exc:
            result["unqualified"][key] = str(exc)
    runtime_labels = sorted({
        record["runtime"]["label"] for _, record, _ in metadata.values()
        if isinstance(record.get("runtime"), dict) and isinstance(record["runtime"].get("label"), str)
    })
    runtime_graph = None
    if runtime_labels:
        # Restrict to selected runtime labels already in the original closure;
        # never introduce the exec protoc or a new default configuration.
        runtime_path = audit.query(
            "proto-original-runtime-archives", "aquery",
            "mnemonic('^(CppArchive|CppLink)$', (" + root_scope + " intersect " + query_labels(runtime_labels) + "))",
            ("--include_artifacts", "--include_aspects"),
        )
        runtime_graph = RuntimeActionGraph(runtime_path, ARCHIVE_ACTIONS, rows, None)
        result["queries"]["runtime_archives"] = sha_file(runtime_path)
    seen = set()
    for name, (key, record, evidence) in metadata.items():
        if key in seen:
            result["unqualified"][key] = "multiple reports for the same configured proto owner"
            result["unqualified_reports"].pop(key, None)
            continue
        seen.add(key)
        row = rows[key]
        invalid_metadata = key in result["unqualified"]
        try:
            if record.get("problems") or record.get("status") == "unsupported":
                raise ValueError("protobuf observer did not support owner: " + repr(record.get("problems")))
            if row["rule_class"] == "cc_proto_library":
                if (key not in selected or record["status"] != "forwarder" or record["compile_actions"]
                        or len(record["direct_proto_deps"]) != 1):
                    raise ValueError("invalid cc_proto_library forwarder")
                child = owner_report(record["direct_proto_deps"][0], metadata, rows)
                if child not in expected:
                    raise ValueError("forwarded proto owner outside original graph")
                result["forwarders"][key] = {"status": "forwarder", "cpp_qualified": False,
                                             "delegates": [child], "evidence": evidence,
                                             "routing_evidence": routing_evidence[name]}
                continue
            cpp = [a for a in original.owners.get(key, []) if a["mnemonic"] == "CppCompile"]
            if any(a["mnemonic"] in ("CppCompileHeader", "CppModuleCompile")
                   for a in original.owners.get(key, [])):
                raise ValueError("protobuf generated header/module compile still needs separate CPP coverage")
            original_generation(key, record, original, native, coverage)
            schema = schema_evidence(key, row, record, original, native, coverage, pp_bep, execroot)
            if record["status"] in ("forwarder", "denylisted"):
                if cpp or record["compile_actions"] or record["generated_sources"] or record["generated_headers"]:
                    raise ValueError("noncompiling proto has original C++ actions")
                if record["status"] == "forwarder" and record["proto_sources"]:
                    raise ValueError("nonempty ProtoInfo claimed as sourceless")
                if record["status"] == "denylisted" and (
                        not record["proto_sources"]
                        or not set(record["proto_sources"]) <= set(record["provided_proto_sources"])):
                    raise ValueError("proto is not actually provided by the selected runtime")
                result["forwarders"][key] = {
                    "status": record["status"], "cpp_qualified": False, "schema": schema,
                    "delegates": [owner_report(dep, metadata, rows) for dep in record["direct_proto_deps"]],
                    "evidence": evidence,
                    "routing_evidence": routing_evidence[name],
                }
                continue
            if record["status"] != "compiled" or not cpp:
                raise ValueError("no real protobuf C++ action")
            recorded = {a["object"]: a for a in record["compile_actions"]}
            if len(recorded) != len(cpp) or len(recorded) != len(record["compile_actions"]):
                raise ValueError("proto observer omitted or added C++ objects")
            headers, header_proof = pp_bep.read_report(execroot, key, record["owned_headers"])
            validate_owned_headers(record, headers)
            allowed, runtime_owner, runtime_native = dependency_owners(
                row, record, metadata, rows, runtime_graph, links, native, coverage)
            entered, uses, findings, unit_proofs = set(), set(), {"public": [], "private": []}, []
            proto_links = [a for a in original.owners.get(key, []) if a["mnemonic"] in ("CppLink", "CppArchive")]
            inputs = compile_inputs(key, original, reports, cpp)
            for action in cpp:
                src, obj = graph_source(original, action)
                if src not in record["generated_sources"] or obj not in recorded:
                    raise ValueError("original proto C++ unit absent from observer")
                exact_recorded_compile(original, action, recorded[obj], inputs[obj])
                compilation = original_compile_reached(key, action, original, native, coverage, proto_links, links)
                pair = bind_preprocessor(key, action, recorded[obj], record, original, reports, produced, inputs[obj])
                if pp_bep.failed(key, pair["preprocess_output"]) or pp_bep.failed(key, pair["header_trace"]):
                    raise ValueError("actual proto compiler preprocessing failed")
                files, missing, used, proof = read_header_uses(key, pair, record, headers, pp_bep, execroot, allowed)
                entered.update(files)
                uses.update(used)
                for visibility in findings:
                    findings[visibility].extend(missing[visibility])
                unit_proofs.append({"source": src, "object": obj, "ordinary": compilation, "preprocessing": proof})
            unentered = sorted(set(headers["required"]) - entered)
            unused_without_schema = [
                dep for dep in record["direct_proto_deps"]
                if owner_report(dep, metadata, rows) not in uses and canonical(dep["label"]) not in schema["retained"]
            ]
            clean = not unentered and not any(findings.values()) and not unused_without_schema
            result["compiled_owners"][key] = {
                "status": "clean" if clean else "findings", "cpp_qualified": clean,
                "cpp_units": unit_proofs, "unentered": unentered, "resolved_findings": findings,
                "unverified_unused_proto_deps": [canonical(dep["label"]) for dep in unused_without_schema],
                "schema": schema, "runtime_owner": runtime_owner["owner"],
                "runtime_ownership": runtime_owner, "runtime_native": runtime_native,
                "evidence": evidence, "headers": header_proof,
                "routing_evidence": routing_evidence[name],
            }
        except (KeyError, TypeError, ValueError, OSError, UnicodeError) as exc:
            result["unqualified"][key] = str(exc)
            # Only a unique, supported proto_library FileWrite already matched
            # to this configured owner and its successful BEP may explain an
            # unqualified child. It never qualifies that child's C++ actions.
            if (not invalid_metadata and row["rule_class"] == "proto_library"
                    and record.get("status") == "compiled" and not record.get("problems")):
                result["unqualified_reports"][key] = {
                    "label": row["label"], "checksum": row["checksum"], "status": "not_proved",
                    "cpp_qualified": False, "reason": str(exc), "evidence": evidence,
                    "routing_evidence": routing_evidence[name],
                }
    missing_selected = set(selected) - set(result["forwarders"])
    for key in missing_selected:
        result["unqualified"].setdefault(key, "no qualified original cc_proto forwarding report")
    needed = {
        key for key in expected if key in original.owners and not rows[key]["is_tool"]
        and any(a["mnemonic"] in ("CppCompile", "CppCompileHeader", "CppModuleCompile")
                for a in original.owners[key])
    }
    for key in needed - set(result["compiled_owners"]):
        result["unqualified"].setdefault(key, "original protobuf C++ lacks an observer/PP report")
    result["scope_complete"] = (
        not result["unqualified"] and needed == set(result["compiled_owners"])
        and all(r["status"] == "clean" for r in result["compiled_owners"].values())
    )
    return result

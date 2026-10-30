#!/usr/bin/env python3
"""Check object forwarding by original cc_library archive actions.

The caller supplies CPP and generated-runtime rows only after their original
reports, BEP completions, source revision and configuration have been checked.
ordinary must describe the same source's non-aspect CppArchive, CppCompile and
Assemble actions, with artifacts, command lines and param-file contents.
It must be constructed by RuntimeActionGraph against original configured rows.
This check cannot qualify C++, assembly, binary hashes, or dependency removal.
"""

from pathlib import PurePosixPath
import re

from audit_full_dwyu import SOURCE_ATTRS, canonical, census_canonical, identity
from audit_proto_dwyu import expanded_args


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _file(name, suffix=None, generated=False):
    path = PurePosixPath(name) if type(name) is str else None
    _require(
        bool(path) and name and not path.is_absolute() and
        ".." not in path.parts and "." not in path.parts and
        (not generated or name.startswith("bazel-out/")) and
        (suffix is None or name.endswith(suffix)),
        "invalid original archive or object filename",
    )
    return name


def _reference(ref):
    _require(
        type(ref) is dict and set(ref) == {"file", "sha256", "bytes"} and
        type(ref["file"]) is str and ref["file"] and
        type(ref["bytes"]) is int and 0 < ref["bytes"] <= 2**31 and
        type(ref["sha256"]) is str and bool(re.fullmatch("[0-9a-f]{64}", ref["sha256"])),
        "missing original evidence reference",
    )


def _original_actions(ordinary, key):
    actions = ordinary.owners.get(key, [])
    _require(actions and all(not a.get("aspectDescriptorIds") for a in actions),
             "original archive or producer actions absent or owned by an aspect")
    return actions


def _object_outputs(ordinary, actions, mnemonic):
    objects = []
    for action in actions:
        _require(action.get("mnemonic") == mnemonic, "producer has unexpected original action")
        outputs = ordinary.files(action, "outputIds")
        selected = [name for name in outputs if name.endswith(".pic.o")]
        _require(len(selected) == 1 and not any(name in ordinary.tree_artifacts for name in outputs),
                 "original producer has missing, ambiguous or tree object outputs")
        objects.append(_file(selected[0], ".pic.o", generated=True))
    _require(len(set(objects)) == len(objects), "producer repeats an original object")
    return objects


def _runtime_objects(ordinary, key, row):
    language = row.get("language")
    _require(
        row.get("status") == "clean" and language in ("C", "CXX", "ASM") and
        row.get("cpp_qualified") is (language != "ASM") and
        row.get("asm_qualified") is (language == "ASM"),
        "runtime producer is not fully qualified for its language",
    )
    _require(identity(row.get("label", ""), row.get("checksum", "")) == key,
             "runtime producer changed original configuration or owner")
    binding = row.get("original_compiler_binding")
    mnemonic = "Assemble" if language == "ASM" else "CppCompile"
    _require(
        type(binding) is dict and binding.get("status") == "matched_original_actions" and
        binding.get("language") == language and binding.get("mnemonic") == mnemonic and
        type(binding.get("units")) is list and binding["units"] and
        type(binding.get("tu_count")) is int and binding["tu_count"] == len(binding["units"]),
        "runtime lacks its original compiler and preprocessing binding",
    )
    queries = binding.get("action_queries")
    _require(type(queries) is dict and set(queries) == {"ordinary", "preprocessing", "declared_inputs"},
             "runtime lacks an original declaration, compiler or preprocessing query")
    for ref in queries.values():
        _reference(ref)
    language_suffixes = {
        "C": (".c",), "CXX": (".cc", ".cpp", ".cxx", ".c++", ".C"), "ASM": (".S",),
    }
    objects, sources, preprocess, headers = [], [], [], []
    for unit in binding["units"]:
        _require(type(unit) is dict and set(unit) == {"source", "object", "preprocessing", "headers"},
                 "invalid original runtime compilation unit")
        source = _file(unit["source"])
        _require(source.endswith(language_suffixes[language]), "runtime source and language differ")
        sources.append(source)
        objects.append(_file(unit["object"], ".pic.o", generated=True))
        preprocess.append(_file(unit["preprocessing"], ".i", generated=True))
        headers.append(_file(unit["headers"], ".headers", generated=True))
    _require(
        sources == sorted(sources) and
        all(len(set(files)) == len(files) for files in (sources, objects, preprocess, headers)),
        "runtime binding duplicates or reorders original units",
    )
    actions = _original_actions(ordinary, key)
    actual = _object_outputs(ordinary, actions, mnemonic)
    _require(set(actual) == set(objects), "runtime binding differs from original compiler objects")
    return set(objects)


def _native_objects(ordinary, key, row):
    _require(
        row.get("identity") == key and row.get("category") == "compiled_native" and
        row.get("status") == "clean" and row.get("cpp_qualified") is True and
        type(row.get("compile_actions")) is int and row["compile_actions"] > 0,
        "native producer is not a fully qualified original C++ target",
    )
    _reference(row.get("original"))
    _require(not any(row.get(field) for field in ("failed_dwyu_actions", "failed_compiler_actions", "holds")),
             "native producer still has a failed or unchecked action")
    _require(type(row.get("findings")) is dict and not any(row["findings"].values()),
             "native producer has unresolved original findings")
    actions = [a for a in _original_actions(ordinary, key) if a["mnemonic"] != "CppArchive"]
    _require(len(actions) == row["compile_actions"], "native compile count changed")
    return set(_object_outputs(ordinary, actions, "CppCompile"))


def check_cc_archive(wrapper, native_rows, runtime_rows, ordinary):
    """Return structural archive verification; keep every original DWYU result.

    native_rows and runtime_rows use the exact configured identity as their key.
    The input reports must already have passed the caller's source and BEP checks.
    Even verified output never permits an input dependency to be removed.
    """
    result = {
        "object_forwarding_verified": False,
        "cpp_qualified": False,
        "asm_qualified": False,
        "original_status": wrapper.get("status"),
        "original": wrapper.get("original"),
        "unchecked_link_inputs": list(wrapper.get("holds", {}).get("unchecked_link_inputs", [])),
    }
    try:
        rule = wrapper.get("original_rule")
        _require(
            type(rule) is dict and rule.get("is_tool") is False and rule.get("rule_class") == "cc_library" and
            wrapper.get("compile_actions") == 0 and wrapper.get("cpp_qualified") is False and
            wrapper.get("category") == "native_with_non_cpp_compile_sources" and
            not wrapper.get("failed_dwyu_actions") and not wrapper.get("failed_compiler_actions"),
            "not an original cc_library archive with no compiler actions",
        )
        key = identity(rule.get("label", ""), rule.get("checksum", ""))
        _require(wrapper.get("identity") == key and canonical(wrapper.get("label", "")) == rule["label"],
                 "wrapper and original rule configurations differ")
        sources = rule.get("sources")
        _require(
            type(sources) is dict and set(sources) == set(SOURCE_ATTRS) and
            type(sources["srcs"]) is int and sources["srcs"] > 0 and
            all(type(sources[name]) is int and sources[name] == 0 for name in sources if name != "srcs") and
            rule.get("actions") == {"CppArchive": 1},
            "archive owns headers, unknown sources or extra original actions",
        )
        _reference(wrapper.get("original"))
        strict = wrapper.get("original_strict")
        _require(type(strict) is dict, "missing original strict completion")
        _reference(strict.get("bep"))
        runs, completions = strict.get("runs"), strict.get("completions")
        _require(
            type(runs) is list and len(runs) == 1 and runs[0].get("bep") == strict["bep"] and
            runs[0].get("invocation_uuid") == strict.get("invocation_uuid") and strict.get("invocation_uuid") and
            type(strict.get("previous_report_removed")) is bool and type(completions) is list and len(completions) == 1,
            "no single original strict build and completion",
        )
        event = completions[0]
        completed, target = event.get("completed", {}), event.get("id", {}).get("targetCompleted", {})
        groups = [g for g in completed.get("outputGroup", []) if g.get("name") == "dwyu"]
        _require(
            canonical(target.get("label", "")) == rule["label"] and
            target.get("configuration", {}).get("id") == rule["checksum"] and
            target.get("aspect", "").startswith("//bazel/rules:dwyu.bzl%dwyu[") and
            completed.get("success") is True and "aborted" not in event and
            len(groups) == 1 and groups[0].get("fileSets"),
            "DWYU did not successfully inspect this archive in its original configuration",
        )
        holds = wrapper.get("holds", {})
        configured = holds.get("configured_unchecked_link_inputs")
        declared = holds.get("unchecked_link_inputs")
        _require(
            type(configured) is list and configured and type(declared) is list and
            all(type(path) is str for path in configured + declared) and
            len(set(configured)) == len(configured) and len(set(declared)) == len(declared) and
            set(configured) == set(declared),
            "original configured and reported archive input sets differ",
        )
        actions = _original_actions(ordinary, key)
        _require(len(actions) == 1 and actions[0].get("mnemonic") == "CppArchive",
                 "missing or ambiguous original archive action")
        archive = actions[0]
        outputs = ordinary.files(archive, "outputIds")
        _require(
            len(outputs) == 1 and outputs[0].endswith((".a", ".lo")) and
            outputs[0] not in ordinary.tree_artifacts,
            "archive has missing, extra or tree outputs",
        )
        argv = expanded_args(archive)
        passed = [arg for arg in argv if arg.endswith((".o", ".obj", ".a", ".lo")) and arg != outputs[0]]
        _require(
            passed and len(set(passed)) == len(passed) and set(passed) == set(configured) and
            all(path in ordinary.inputs(archive) and path not in ordinary.tree_artifacts for path in passed),
            "archive arguments omit, repeat or add original objects",
        )
        nm = wrapper.get("wrapper_link_objects")
        _require(type(nm) is list and len(nm) == len(configured), "missing original object NM records")
        seen, by_producer = set(), {}
        symbols = {"strong_definitions", "strong_undefined", "weak_definitions", "weak_undefined", "unsupported"}
        for obj in nm:
            _require(
                type(obj) is dict and set(obj) == symbols | {"object", "producer", "link_owner", "malformed"} and
                type(obj["producer"]) is str and obj["producer"] and
                canonical(obj["link_owner"]) == rule["label"] and obj["malformed"] is False,
                "malformed original NM or foreign archive owner",
            )
            path = _file(obj["object"], ".pic.o", generated=True)
            _require(path in configured and path not in seen, "unknown or repeated NM object")
            seen.add(path)
            for field in symbols:
                names = obj[field]
                _require(type(names) is list and all(type(s) is str and s for s in names) and
                         len(set(names)) == len(names), "malformed NM symbol list")
            _require(not obj["unsupported"], "unsupported original NM symbol type")
            producer = identity(census_canonical(obj["producer"]), rule["checksum"])
            by_producer.setdefault(producer, set()).add(path)
        _require(seen == set(configured), "NM records do not cover every original object")
        for producer, forwarded in by_producer.items():
            native, runtime = native_rows.get(producer), runtime_rows.get(producer)
            _require((native is None) != (runtime is None), "unknown or ambiguous object producer")
            expected = (_runtime_objects(ordinary, producer, runtime) if runtime is not None
                        else _native_objects(ordinary, producer, native))
            _require(expected == forwarded, "archive omitted or added producer objects")
        result["object_forwarding_verified"] = True
        result["unchecked_link_inputs"] = []
        result["objects"] = passed
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        result["reason"] = str(exc)
    return result

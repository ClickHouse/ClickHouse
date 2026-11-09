"""Check completed zlib runtime status against bound, actual CppLink actions."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read(path):
    raw = path.read_bytes()
    return json.loads(raw), {"path": str(path), "bytes": len(raw), "sha256": sha(raw)}


def link_arguments(action):
    # --include_param_files describes arguments of a referenced response file;
    # do not read or guess contents from an execroot which may have evicted it.
    params = {p["execPath"]: p["arguments"] for p in action.get("paramFiles", [])}

    def expand(args, parents=()):
        for arg in args:
            if arg.startswith("@"):
                name = arg[1:]
                if name not in params or name in parents:
                    raise ValueError("unbound/recursive CppLink response file: " + arg)
                yield from expand(params[name], (*parents, name))
            elif arg.startswith("-Wl,"):
                yield from arg[4:].split(",")
            elif arg != "-Xlinker":
                yield arg

    return list(expand(action["arguments"]))


def argument_states(args):
    result = []
    whole = False
    in_start_lib = False
    stack = []
    for index, arg in enumerate(args):
        if arg == "--push-state":
            stack.append((whole, in_start_lib))
        elif arg == "--pop-state":
            if not stack:
                raise ValueError("unbalanced --pop-state in CppLink")
            whole, in_start_lib = stack.pop()
        elif arg in ("--whole-archive", "-whole-archive"):
            whole = True
        elif arg in ("--no-whole-archive", "-no-whole-archive"):
            whole = False
        elif arg == "--start-lib":
            if in_start_lib:
                raise ValueError("nested --start-lib in CppLink")
            in_start_lib = True
        elif arg == "--end-lib":
            if not in_start_lib:
                raise ValueError("unbalanced --end-lib in CppLink")
            in_start_lib = False
        result.append((index, arg, whole, in_start_lib))
    if stack or in_start_lib:
        raise ValueError("unterminated linker state in CppLink")
    return result


def required_libraries(records):
    result = []
    for record in records:
        for lib in record["libraries"]:
            variants = {x for x in (lib["static"], lib["pic_static"]) if x}
            if not variants:
                raise ValueError("configured runtime did not expose a static library: " + record["owner"])
            result.append((record["owner"], lib, variants))
    if not result:
        raise ValueError("configured runtime has no static libraries")
    return result


def positive_link(action, libraries):
    states = argument_states(link_arguments(action))
    positions = {}
    for state in states:
        positions.setdefault(state[1], []).append(state)
    results = []
    last = -1
    for owner, lib, variants in libraries:
        archive_states = [s for name in variants for s in positions.get(name, ())]
        pic = lib["pic_objects"]
        objects = lib["objects"]
        object_paths = set(pic + objects)
        object_states = [s for name in object_paths for s in positions.get(name, ())]
        if archive_states:
            if len(archive_states) != 1 or object_states:
                raise ValueError("duplicate runtime archive/objects in link for " + owner)
            state = archive_states[0]
            if lib["alwayslink"] and (not state[2] or state[3]):
                raise ValueError("alwayslink runtime archive is not in --whole-archive for " + owner)
            first = state[0]
            result = {"owner": owner, "mode": "archive", "file": state[1],
                      "position": first, "alwayslink": lib["alwayslink"]}
        else:
            present = Counter(s[1] for s in object_states)
            expected_pic = Counter(pic) if pic else None
            expected_objects = Counter(objects) if objects else None
            if present not in (expected_pic, expected_objects):
                raise ValueError("missing, mixed, or duplicate runtime objects for %s: %s" %
                                 (owner, dict(present)))
            if not object_states:
                raise ValueError("runtime library not found in CppLink: " + owner)
            if lib["alwayslink"] and any(in_lib and not whole for _, _, whole, in_lib in object_states):
                raise ValueError("alwayslink runtime objects are lazy --start-lib members for " + owner)
            first = min(s[0] for s in object_states)
            result = {"owner": owner, "mode": "objects", "count": len(object_states),
                      "position": first, "alwayslink": lib["alwayslink"]}
        if first <= last:
            raise ValueError("CppLink changed configured runtime library order at " + owner)
        last = first
        results.append(result)
    return results


def negative_link(action, libraries):
    tokens = set(link_arguments(action))
    found = []
    # A normal toolchain can supply its own compiler builtins. These exact
    # filenames come from ClickHouse's configured CcInfo, never a name glob.
    for owner, lib, variants in libraries:
        matches = sorted(tokens.intersection(variants | set(lib["pic_objects"]) | set(lib["objects"])))
        if matches:
            found.append({"owner": owner, "files": matches})
    if found:
        raise ValueError("ClickHouse runtime propagated into a negative link: " + str(found))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--configured", type=Path, required=True)
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--aquery", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cfg, cfg_record = read(args.configured)
    status, status_record = read(args.status)
    query, query_record = read(args.aquery)
    if status["configured_sha256"] != cfg_record["sha256"] or status["arch"] != cfg["arch"]:
        raise ValueError("native status is not bound to the configured CcInfo")
    if set(status["programs"]) != {"binary", "test"} or any(
        program["exit_code"] != 0 or program["preinit_entry"]["value"] != program["xray_init_address"]
        for program in status["programs"].values()
    ):
        raise ValueError("actual native executables have not both passed")
    fragments = {f["id"]: f for f in query["pathFragments"]}
    artifacts = {a["id"]: a for a in query["artifacts"]}

    def artifact_path(artifact_id):
        fragment = artifacts[artifact_id]["pathFragmentId"]
        names = []
        while fragment:
            entry = fragments[fragment]
            names.append(entry["label"])
            fragment = entry.get("parentId")
        return "/".join(reversed(names))

    by_output = {}
    for action in query["actions"]:
        if action["mnemonic"] != "CppLink":
            raise ValueError("expected bounded CppLink-only aquery")
        primary = artifact_path(action["primaryOutputId"])
        if primary in by_output:
            raise ValueError("two CppLink actions claim " + primary)
        by_output[primary] = action
    variants = cfg["outputs"]
    if set(variants) != {"binary", "test", "shared", "explicit_empty", "exec_binary"}:
        raise ValueError("incomplete configured link controls")
    libraries = required_libraries(cfg["required_runtime"])
    exec_libraries = required_libraries(cfg["exec_required_runtime"])
    results = {}
    seen = set()
    for name, outputs in variants.items():
        candidates = [path for path in outputs if path in by_output]
        if len(candidates) != 1 or candidates[0] in seen:
            raise ValueError("missing/ambiguous configured CppLink %s: %s" % (name, candidates))
        primary = candidates[0]
        seen.add(primary)
        action = by_output[primary]
        if name in ("binary", "test"):
            detail = positive_link(action, libraries)
        else:
            negative_link(action, exec_libraries if name == "exec_binary" else libraries)
            detail = []
        results[name] = {"primary_output": primary, "cpp_link_action_key": action["actionKey"],
                         "verified_runtime_inputs": detail}
    args.output.write_text(json.dumps({
        "configured": cfg_record, "native_status": status_record, "aquery": query_record,
        "arch": cfg["arch"], "links": results, "public_zlib_runtime_checked": True,
        "clickhouse_hashes_matched": False, "cpp_qualified": False, "whole_repository_clean": False,
    }, sort_keys=True, indent=2) + "\n")


if __name__ == "__main__":
    main()

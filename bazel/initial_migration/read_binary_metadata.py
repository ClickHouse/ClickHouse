#!/usr/bin/env python3
"""Compare the normalized ClickHouse BEP digest and size without fetching either ELF.

Run from the repository root:
  bazel build --config=cmake_match --build_event_json_file=/tmp/ch.bep \
      //:clickhouse //bazel/initial_migration:clickhouse.normalized
  python3 bazel/initial_migration/read_binary_metadata.py --arch x86_64 /tmp/ch.bep

Use --config=cmake_match_aarch64 and --arch aarch64 for ARM64.
"""

import argparse
import json
from pathlib import Path
import re
import sys

from audit_full_dwyu import bep_file_matches, canonical


EXPECTED = {
    "x86_64": {"sha256": "ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585", "bytes": 710693736},
    "aarch64": {"sha256": "a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03", "bytes": 637316696},
}
TARGETS = {
    "//:clickhouse": "clickhouse",
    "//bazel/initial_migration:clickhouse.normalized": "bazel/initial_migration/clickhouse.normalized",
}
BYTESTREAM = re.compile(
    r"bytestream://[A-Za-z0-9][A-Za-z0-9.-]*(?::[0-9]+)?"
    r"(?:/[A-Za-z0-9][A-Za-z0-9._-]*)*/blobs/([0-9a-f]{64})/(0|[1-9][0-9]*)"
)


def read_metadata(bep, arch):
    started = []
    finished = []
    sets = {}
    completions = {label: [] for label in TARGETS}
    for line in bep:
        event = json.loads(line)
        ident = event["id"]
        if "started" in ident:
            started.append(event["started"])
        if "buildFinished" in ident:
            finished.append(event["finished"])
        if "namedSet" in ident:
            key = ident["namedSet"]["id"]
            if key in sets:
                raise ValueError("duplicate BEP named set " + key)
            sets[key] = event["namedSetOfFiles"]
        completed = ident.get("targetCompleted")
        if completed and not completed.get("aspect"):
            label = canonical(completed.get("label", ""))
            if label in completions:
                completions[label].append(event)
    if (len(started) != 1 or not started[0].get("uuid") or
            started[0].get("command") != "build" or len(finished) != 1):
        raise ValueError("BEP must contain one complete Bazel build")
    code = finished[0].get("exitCode", {})
    if code.get("name") != "SUCCESS" or code.get("code", 0) != 0:
        raise ValueError("Bazel build failed: " + str(code))
    result = {"invocation_id": started[0]["uuid"], "arch": arch, "files": {}}
    configurations = set()
    for label, name in TARGETS.items():
        events = completions[label]
        if len(events) != 1 or events[0]["completed"].get("success") is not True:
            raise ValueError("BEP lacks one successful completion for " + label)
        event = events[0]
        configurations.add(event["id"]["targetCompleted"]["configuration"]["id"])
        todo = [item["id"] for group in event["completed"].get("outputGroup", [])
                if group.get("name") == "default" for item in group.get("fileSets", [])]
        visited = set()
        matches = []
        while todo:
            key = todo.pop()
            if key in visited:
                continue
            visited.add(key)
            entry = sets[key]
            todo.extend(item["id"] for item in entry.get("fileSets", []))
            for item in entry.get("files", []):
                relative = Path(*item.get("pathPrefix", [])) / item["name"]
                if str(relative) == name or str(relative).endswith("/bin/" + name):
                    matches.append(item)
        if len(matches) != 1:
            raise ValueError("BEP lacks a unique default output file for " + label)
        item = matches[0]
        uri = item.get("uri", "")
        match = BYTESTREAM.fullmatch(uri)
        if not match:
            raise ValueError("BEP has no remote SHA-256 blob URI for " + name)
        metadata = {"sha256": match[1], "bytes": int(match[2])}
        if metadata["bytes"] == 0 or not bep_file_matches(item, metadata):
            raise ValueError("BEP digest or size contradicts blob URI for " + name)
        result["files"][label] = metadata
    if len(configurations) != 1:
        raise ValueError("ClickHouse and the normalized ELF have different Bazel configurations")
    result["configuration"] = configurations.pop()
    reference = EXPECTED[arch]
    result["recorded_cmake_normalized_sha256"] = reference["sha256"]
    result["recorded_cmake_normalized_bytes"] = reference["bytes"]
    result["matches_cmake"] = result["files"]["//bazel/initial_migration:clickhouse.normalized"] == reference
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arch", required=True, choices=EXPECTED)
    parser.add_argument("bep", type=Path)
    args = parser.parse_args()
    try:
        with args.bep.open() as inp:
            result = read_metadata(inp, args.arch)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["matches_cmake"] else 1
    except (ValueError, KeyError, TypeError, OSError) as error:
        print("Invalid binary metadata: " + str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

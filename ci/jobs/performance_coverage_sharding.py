"""Balance whole performance XML files using instrumented collection runtimes."""

import argparse
import hashlib
import json
import math
import os
import statistics
import xml.etree.ElementTree as ET
from pathlib import Path

from ci.jobs.scripts.perf import test_discovery

TIMINGS_PATH = Path(__file__).with_name("performance_coverage_timings.json")


def test_identity(path):
    """Hash XML, recursive fragments, SQL and settings using runner path rules."""
    path = path.resolve()
    inputs = {path: path.read_bytes()}

    def visit(parent, directory, stack):
        for element in parent:
            reference = element.get("file")
            if element.tag == "include":
                if not reference or len(element.attrib) != 1:
                    raise ValueError(
                        '<include> requires exactly one attribute: file="..."'
                    )
                target = (directory / reference).resolve()
                if target in stack:
                    raise ValueError(f"Cyclic performance-test include: {target}")
                inputs[target] = target.read_bytes()
                fragment = ET.fromstring(inputs[target])
                if fragment.tag != "fragment":
                    raise ValueError(
                        f"Performance-test include {target} must have a <fragment> root"
                    )
                visit(fragment, target.parent, (*stack, target))
            else:
                if element.tag in ("query", "settings") and reference:
                    target = (directory / reference).resolve()
                    inputs[target] = target.read_bytes()
                visit(element, directory, stack)

    visit(ET.fromstring(inputs[path]), path.parent, (path,))
    digest = hashlib.sha256()
    for source, content in sorted(inputs.items()):
        digest.update(os.path.relpath(source, path.parent).encode() + b"\0")
        digest.update(hashlib.sha256(content).digest())
    return digest.hexdigest()


def load_timings(path=TIMINGS_PATH):
    data = json.loads(path.read_text())
    if data["version"] != 1 or data["warm_runs"] != 1:
        raise ValueError("Expected first-plus-one-warm coverage timings")
    values = [data["job_overhead_seconds"]]
    values.extend(row["seconds"] for row in data["tests"].values())
    if not data["tests"] or any(
        isinstance(value, bool) or not math.isfinite(value) or value <= 0
        for value in values
    ):
        raise ValueError("Coverage runtimes must be finite positive seconds")
    return data


def assign_tests(paths, total, timings):
    """Longest tests first; new tests use the measured table's upper quartile."""
    if total < 1 or len({path.name for path in paths}) != len(paths):
        raise ValueError("Invalid shard count or duplicate XML names")
    # A new XML is always admitted, with an explicit nonzero estimate. Discovery,
    # rather than this historical timing table, defines the current test scope.
    values = [row["seconds"] for row in timings["tests"].values()]
    new_test_seconds = (
        statistics.quantiles(values, n=4, method="inclusive")[2]
        if len(values) > 1
        else values[0]
    )
    durations = {}
    excluded = []
    for path in sorted(paths):
        if test_discovery.test_is_long(path) or test_discovery.test_has_shell_query(
            path
        ):
            excluded.append(path)
        else:
            identity = test_identity(path)
            previous = timings["tests"].get(path.name)
            if previous is None:
                row = {"seconds": new_test_seconds, "kind": "new_test_estimate"}
            elif previous["input_sha256"] != identity:
                # Do not turn a previously expensive test into a cheap test
                # merely because its inputs changed. The next full run will
                # supply its new measured runtime.
                row = {
                    "seconds": max(new_test_seconds, previous["seconds"]),
                    "kind": "changed_test_estimate",
                }
                if previous.get("isolate_until_measured"):
                    row["isolate_until_measured"] = True
            else:
                row = dict(previous)
            durations[path.name] = {**row, "input_sha256": identity}
    buckets = [[] for _ in range(total)]
    loads = [timings["job_overhead_seconds"] for _ in range(total)]
    isolated = sorted(
        path
        for path in paths
        if path.name in durations and durations[path.name].get("isolate_until_measured")
    )
    if len(isolated) > total or (
        len(isolated) == total and len(isolated) < len(durations)
    ):
        raise ValueError("Not enough shards to isolate tests without measured runtimes")
    for shard, path in enumerate(isolated):
        buckets[shard].append(path)
        loads[shard] += durations[path.name]["seconds"]
    for path in sorted(
        (path for path in paths if path.name in durations and path not in isolated),
        key=lambda path: (-durations[path.name]["seconds"], path.name),
    ):
        shard = min(
            range(len(isolated), total), key=lambda index: (loads[index], index)
        )
        buckets[shard].append(path)
        loads[shard] += durations[path.name]["seconds"]
    # Keep explicit exclusions in the manifests, without giving skipped XMLs
    # the weight of a running test or allowing them to move eligible tests.
    for index, path in enumerate(excluded):
        buckets[index % total].append(path)
    assignment = {
        path.name: index + 1 for index, bucket in enumerate(buckets) for path in bucket
    }
    if len(assignment) != len(paths):
        raise ValueError("Coverage assignment must contain every XML exactly once")
    digest = hashlib.sha256(json.dumps(assignment, sort_keys=True).encode()).hexdigest()
    return buckets, {
        "strategy": "longest_runtime_first_v1",
        "isolated_until_measured": [path.name for path in isolated],
        "shards": total,
        "assignment_sha256": digest,
        "predicted_job_seconds": loads,
        "job_overhead_seconds": timings["job_overhead_seconds"],
        "new_test_estimate_seconds": new_test_seconds,
        "new_tests": sorted(
            name
            for name, row in durations.items()
            if row["kind"] == "new_test_estimate"
        ),
        "changed_tests": sorted(
            name
            for name, row in durations.items()
            if row["kind"] == "changed_test_estimate"
        ),
        "excluded_tests": [path.name for path in excluded],
        "timings": durations,
    }


def update_timings(manifests, destination):
    """Refresh weights from completed job manifests; restored durations are ignored."""
    data = load_timings(destination)
    observed = {}
    identities = set()
    for path in manifests:
        manifest = json.loads(path.read_text())
        if not manifest["complete"] or manifest["warm_runs"] != 1:
            raise ValueError(f"Incomplete or incompatible coverage manifest: {path}")
        identities.add((manifest["binary_sha256"], manifest["source_identity"]))
        for name, result in manifest["tests"].items():
            if result["status"] == "skipped":
                continue
            if result["status"] != "collected":
                raise ValueError(f"Incomplete test: {name}")
            seconds = result.get("wall_seconds")
            if result.get("resumed_from") or seconds is None:
                continue
            if isinstance(seconds, bool) or not math.isfinite(seconds) or seconds <= 0:
                raise ValueError(f"Invalid test runtime: {name}")
            identity = manifest["sharding"]["timings"][name]["input_sha256"]
            observed.setdefault((name, identity), []).append(seconds)
    if len(identities) != 1 or not observed:
        raise ValueError("Expected fresh timings from compatible coverage manifests")
    if len({name for name, _ in observed}) != len(observed):
        raise ValueError("Conflicting test inputs in compatible coverage manifests")
    for (name, identity), values in observed.items():
        data["tests"][name] = {
            "seconds": statistics.median(values),
            "kind": "measured_wall",
            "input_sha256": identity,
        }
    data["latest_refresh_identity"] = dict(
        zip(("binary_sha256", "source_identity"), identities.pop())
    )
    destination.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--update-timings", nargs="+", type=Path, metavar="MANIFEST")
    mode.add_argument("--show-plan", action="store_true")
    parser.add_argument("--shards", type=int, default=16)
    parser.add_argument("--timings", type=Path, default=TIMINGS_PATH)
    args = parser.parse_args()
    if args.show_plan:
        paths = [
            Path("tests/performance") / name
            for name in test_discovery.list_test_files("tests/performance")
        ]
        buckets, plan = assign_tests(paths, args.shards, load_timings(args.timings))
        plan["assignment"] = {
            path.name: index + 1
            for index, bucket in enumerate(buckets)
            for path in bucket
        }
        print(json.dumps(plan, indent=2, sort_keys=True))
    else:
        update_timings(args.update_timings, args.timings)

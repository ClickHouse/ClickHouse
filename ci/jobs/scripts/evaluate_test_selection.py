"""Replay JSONL cases using pre-PR snapshots and the production query/scorer.

Each case supplies `pr`, `cutoff`, `diff`, `snapshots`, `regions`, `changed_tests`,
`previously_failed_tests`, and `labels` (test -> changed/failure/flaky_fix/regression).
Controls have an empty `labels` object. `--query-url` fetches snapshots/features
at each cutoff instead of reading them from the case. Label provenance belongs
in `label_sources` as full issue/PR links. Future observations are rejected.
"""

import argparse
import json
from datetime import timedelta
from pathlib import Path
from statistics import mean
from time import monotonic

from ci.jobs.scripts.coverage_selection import (
    build_candidate_query,
    parse_rows,
    protect_selection,
    rank_candidates,
    snapshot_query,
    timestamp,
    validate_snapshots,
)
from ci.jobs.scripts.find_tests import Targeting
from ci.jobs.scripts.test_selection_config import SELECTION_CONFIG


def quantile(values, fraction):
    values = sorted(values)
    position = (len(values) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def evaluate_case(case, cidb=None):
    cutoff = case["cutoff"]
    lines = [
        (path, line)
        for path, line in Targeting._parse_diff_lines(case["diff"])
        if path.startswith(("src/", "programs/", "utils/", "base/"))
        and path not in Targeting.SHARED_REGISTRY_FILES
    ]
    hunks = Targeting._parse_diff_hunk_ranges(case["diff"])
    snapshots = (
        parse_rows(cidb.query(snapshot_query(cutoff), log_level=""))
        if cidb
        else case["snapshots"]
    )
    validate_snapshots(snapshots, cutoff)
    query = build_candidate_query(lines, hunks, snapshots) if lines else None
    started = monotonic()
    if cidb:
        raw = cidb.query(query, log_level="") if query else ""
        regions = parse_rows(raw)
        response_bytes = len(raw.encode())
    else:
        regions = case["regions"]
        response_bytes = len(json.dumps(regions).encode())
    query_seconds = monotonic() - started if cidb else case.get("query_seconds")
    labels = {
        Targeting.selection_test_name(test): kind
        for test, kind in case["labels"].items()
    }
    if any(
        kind not in ("changed", "failure", "flaky_fix", "regression")
        for kind in labels.values()
    ):
        raise ValueError("Unknown evaluation label")
    # Mandatory changed tests do not establish the coverage scorer's recall.
    relevant = {test for test, kind in labels.items() if kind != "changed"}
    modes = {}
    for mode in ("disabled", "legacy-tier", "relative-low", "relative-high"):
        candidates = rank_candidates(regions, lines, hunks, snapshots, entry_mode=mode)
        selection = protect_selection(
            case["changed_tests"],
            case["previously_failed_tests"],
            candidates,
            Targeting.selection_test_name,
        )
        ranked = [record["test"] for record in selection["selected"]]
        coverage_ranked = list(
            dict.fromkeys(Targeting.selection_test_name(c["test"]) for c in candidates)
        )
        found = relevant.intersection(ranked)
        modes[mode] = {
            "selected_count": len(ranked),
            "ceiling_truncated": selection["ceiling_truncated"],
            "recall": {
                str(k): (
                    len(relevant.intersection(ranked[:k])) / len(relevant)
                    if relevant
                    else None
                )
                for k in (25, 50, 100, 250)
            },
            "coverage_recall": {
                str(k): (
                    len(relevant.intersection(coverage_ranked[:k])) / len(relevant)
                    if relevant
                    else None
                )
                for k in (25, 50, 100, 250)
            },
            "reciprocal_rank": (
                mean(
                    1 / (ranked.index(test) + 1) if test in ranked else 0
                    for test in relevant
                )
                if relevant
                else None
            ),
            "false_negatives": {
                test: (
                    "temporary_ceiling"
                    if test in coverage_ranked
                    else "no_precise_coverage"
                )
                for test in sorted(relevant - found)
            },
            "contribution": {
                source: sum(
                    record["source"] == source for record in selection["selected"]
                )
                for source in ("changed", "previously_failed", "primary_coverage")
            },
            "tests": ranked,
        }
    # Removing each independent snapshot measures ranking stability without
    # counting duplicate randomized observations as stronger coverage evidence.
    stability = []
    base = set(modes["disabled"]["tests"])
    for snapshot in snapshots:
        key = [snapshot["check_start_time"], snapshot["check_name"]]
        reduced = []
        for region in regions:
            observations = [row for row in region["observations"] if row[1:3] != key]
            if observations:
                reduced.append(
                    {
                        **region,
                        "observations": observations,
                        "region_owners": len({row[0] for row in observations}),
                    }
                )
        candidates = rank_candidates(reduced, lines, hunks, snapshots)
        selection = protect_selection(
            case["changed_tests"],
            case["previously_failed_tests"],
            candidates,
            Targeting.selection_test_name,
        )
        tests = {record["test"] for record in selection["selected"]}
        stability.append(len(base & tests) / len(base | tests) if base | tests else 1)
    return {
        "pr": case["pr"],
        "cutoff": cutoff,
        "label_sources": case.get("label_sources", {}),
        "label_kinds": sorted(set(labels.values())),
        "control": not labels,
        "query": query,
        "query_seconds": query_seconds,
        "response_bytes": response_bytes,
        "snapshot_leave_one_out_jaccard": stability,
        "modes": modes,
    }


def evaluate(cases, cidb=None):
    if not cases:
        raise ValueError("Replay requires evaluation cases")
    results = [evaluate_case(case, cidb) for case in cases]
    modes = {}
    for mode in results[0]["modes"]:
        scores = [result["modes"][mode] for result in results]
        counts = [score["selected_count"] for score in scores]
        modes[mode] = {
            "selected_count": {
                "median": quantile(counts, 0.5),
                "p80": quantile(counts, 0.8),
                "p90": quantile(counts, 0.9),
                "max": max(counts),
            },
            "fraction_below_100": mean(
                count < SELECTION_CONFIG.selection_target for count in counts
            ),
        }
        for metric in ("recall", "coverage_recall"):
            modes[mode][metric] = {}
            for k in (25, 50, 100, 250):
                values = [
                    score[metric][str(k)]
                    for score in scores
                    if score[metric][str(k)] is not None
                ]
                modes[mode][metric][str(k)] = mean(values) if values else None
    times = [timestamp(case["cutoff"]) for case in cases]
    kinds = {kind for result in results for kind in result["label_kinds"]}
    return {
        "selector_version": SELECTION_CONFIG.version,
        "cases": results,
        "summary": modes,
        "evaluation_window_days": (max(times) - min(times)).total_seconds() / 86400,
        "quality_gate_ready_for_review": (
            max(times) - min(times) >= timedelta(days=60)
            and {"failure", "flaky_fix", "regression"}.issubset(kinds)
            and any(result["control"] for result in results)
            and all(case.get("label_sources") for case in cases if case["labels"])
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cases", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--query-url")
    args = parser.parse_args()
    cidb = None
    if args.query_url:
        from ci.praktika.cidb import CIDB

        cidb = CIDB(args.query_url)
    cases = [
        json.loads(line) for line in args.cases.read_text().splitlines() if line.strip()
    ]
    result = evaluate(cases, cidb)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {key: value for key, value in result.items() if key != "cases"}, indent=2
        )
    )


if __name__ == "__main__":
    main()

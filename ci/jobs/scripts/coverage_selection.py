"""Coverage contract, query construction and scoring shared by CI and replay."""

import json
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median

from ci.jobs.scripts.test_selection_config import SELECTION_CONFIG


def canonical_coverage_path(path):
    path = path.replace("\\", "/")
    while path.startswith("./"):
        path = path[2:]
    if (
        not path
        or path.startswith("/")
        or ":" in path
        or any(part in ("", ".", "..") for part in path.split("/"))
    ):
        raise ValueError(f"Invalid repository-relative coverage path: {path!r}")
    return path


def canonical_coverage_paths(path):
    bare = canonical_coverage_path(path)
    return bare, "./" + bare


def sql_string(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def snapshot_predicate(snapshots):
    if not snapshots:
        raise ValueError("Coverage snapshot is empty")
    keys = ", ".join(
        f"(toDateTime({sql_string(s['check_start_time'])}, 'UTC'), {sql_string(s['check_name'])})"
        for s in snapshots
    )
    return f"(check_start_time, check_name) IN ({keys})"


def snapshot_query(cutoff, config=SELECTION_CONFIG):
    # Temporary identity until CIDB has a workflow run/shard metadata table.
    # Select independent observations per shard; hours are never workflow IDs.
    return f"""
        SELECT check_start_time, check_name, uniqExact(test_name) AS exported_tests
        FROM checks_coverage_lines
        WHERE check_start_time <= toDateTime({sql_string(cutoff)}, 'UTC')
          AND check_start_time > toDateTime({sql_string(cutoff)}, 'UTC')
              - INTERVAL {config.coverage_search_days} DAY
          AND check_name LIKE 'Stateless%per_test_coverage%'
          AND match(test_name, '^[0-9]{{5}}_')
        GROUP BY check_start_time, check_name
        HAVING exported_tests >= {config.min_exported_tests_per_shard}
        ORDER BY check_start_time DESC, check_name
        LIMIT {config.coverage_run_count} BY check_name
        FORMAT JSONEachRow
    """


def validate_snapshots(snapshots, cutoff, config=SELECTION_CONFIG):
    import re

    newest = {}
    cutoff_time = timestamp(cutoff)
    for snapshot in snapshots:
        when = timestamp(snapshot["check_start_time"])
        if when > cutoff_time:
            raise ValueError("Coverage observation is newer than the evaluation cutoff")
        if when <= cutoff_time - timedelta(days=config.coverage_search_days):
            raise ValueError("Coverage observation is outside the supported window")
        if int(snapshot["exported_tests"]) < config.min_exported_tests_per_shard:
            raise ValueError(f"Unhealthy coverage snapshot: {snapshot}")
        shard = re.search(r", (\d+)/(\d+)\)$", snapshot["check_name"])
        if not shard or int(shard[2]) != config.coverage_shards:
            raise ValueError(f"Unexpected coverage shard: {snapshot['check_name']}")
        number = int(shard[1])
        newest[number] = max(newest.get(number, when), when)
    expected = set(range(1, config.coverage_shards + 1))
    if set(newest) != expected:
        raise ValueError(
            f"Missing healthy coverage shards: {sorted(expected - set(newest))}"
        )
    stale = [
        n
        for n, when in newest.items()
        if cutoff_time - when > timedelta(hours=config.coverage_max_age_hours)
    ]
    if stale:
        raise ValueError(f"Stale coverage shards: {stale}; newest timestamps: {newest}")


def build_candidate_query(
    changed_lines, hunk_ranges, snapshots, config=SELECTION_CONFIG
):
    files = defaultdict(set)
    for path, line in changed_lines:
        files[canonical_coverage_path(path)].add(int(line))
    conditions = []
    for path, lines in sorted(files.items()):
        paths = ", ".join(map(sql_string, canonical_coverage_paths(path)))
        ranges = list(hunk_ranges.get(path, [])) + [
            (line, line) for line in sorted(lines)
        ]
        merged = []
        for start, end in sorted(set(ranges)):
            start, end = max(0, start), max(start, end)
            if merged and start <= merged[-1][1] + 1:
                merged[-1][1] = max(merged[-1][1], end)
            else:
                merged.append([start, end])
        overlaps = " OR ".join(
            f"(line_end >= {start} AND line_start <= {end})" for start, end in merged
        )
        conditions.append(f"(file IN ({paths}) AND ({overlaps}))")
    if not conditions:
        raise ValueError("Candidate query needs changed coverage lines")
    # Broad-region features may later corroborate or append after frozen precise
    # results, but broad-only admission requires replay proving recall near 100.
    return f"""
        WITH per_run_region_test AS
        (
            SELECT if(startsWith(file, './'), substring(file, 3), file) AS canonical_file,
                   line_start, line_end, test_name, check_start_time, check_name,
                   medianExact(min_depth) AS entry_count
            FROM checks_coverage_lines
            WHERE {snapshot_predicate(snapshots)}
              AND match(test_name, '^[0-9]{{5}}_')
              AND line_end >= line_start
              AND ({' OR '.join(conditions)})
            GROUP BY canonical_file, line_start, line_end, test_name, check_start_time, check_name
        )
        SELECT canonical_file AS file, line_start, line_end,
               uniqExact(test_name) AS region_owners,
               groupArray((test_name, toString(check_start_time), check_name, entry_count)) AS observations
        FROM per_run_region_test
        GROUP BY canonical_file, line_start, line_end
        HAVING line_end - line_start + 1 <= {config.narrow_region_max_lines}
           AND region_owners <= {config.max_precise_region_owners}
        ORDER BY file, line_start, line_end
        FORMAT JSONEachRow
    """


def parse_rows(raw):
    if raw is None:
        raise RuntimeError("Coverage query returned no response")
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def rank_candidates(
    regions,
    changed_lines,
    hunk_ranges,
    snapshots,
    config=SELECTION_CONFIG,
    entry_mode="disabled",
):
    if entry_mode not in ("disabled", "relative-low", "relative-high", "legacy-tier"):
        raise ValueError(f"Unknown entry-count experiment: {entry_mode}")
    snapshot_keys = {(s["check_start_time"], s["check_name"]) for s in snapshots}
    changed = defaultdict(set)
    for path, line in changed_lines:
        changed[canonical_coverage_path(path)].add(line)
    candidates = {}
    seen_regions = set()
    for region in regions:
        path = canonical_coverage_path(region["file"])
        start, end = int(region["line_start"]), int(region["line_end"])
        width, owners = end - start + 1, int(region["region_owners"])
        region_id = f"{path}:{start}-{end}"
        if region_id in seen_regions:
            raise ValueError(f"Duplicate aggregated region: {region_id}")
        seen_regions.add(region_id)
        observations = defaultdict(dict)
        for test, observed_at, check_name, entry_count in region["observations"]:
            key = (observed_at, check_name)
            if key not in snapshot_keys:
                raise ValueError(f"Coverage row outside selected snapshots: {key}")
            if not 0 <= entry_count <= 255:
                raise ValueError(f"Invalid entry count: {entry_count}")
            if key in observations[test]:
                raise ValueError(f"Duplicate per-run observation for {test}: {key}")
            observations[test][key] = entry_count
        if owners != len(observations) or width < 1:
            raise ValueError(f"Invalid region features: {region_id}")
        if (
            width > config.narrow_region_max_lines
            or owners > config.max_precise_region_owners
        ):
            continue
        exact = sorted(line for line in changed[path] if start <= line <= end)
        hunks = [
            f"{path}:{a}-{b}"
            for a, b in hunk_ranges.get(path, [])
            if start <= max(a, b) and end >= a
        ]
        if not exact and not hunks:
            continue
        weight = len(exact) if exact else config.hunk_context_weight
        counts = {
            test: median(math.log1p(value) for value in runs.values() if value != 255)
            for test, runs in observations.items()
            if any(value != 255 for value in runs.values())
        }
        for test, runs in sorted(observations.items()):
            relative = 0.5
            if test in counts and len(counts) > 1:
                # Tied censored values (254 means >=254) receive the same midrank.
                relative = (
                    sum(value < counts[test] for value in counts.values())
                    + (sum(value == counts[test] for value in counts.values()) - 1) / 2
                ) / (len(counts) - 1)
            multiplier = 1.0
            if entry_mode.startswith("relative-"):
                direction = 1 if entry_mode == "relative-high" else -1
                multiplier += (
                    direction * config.entry_count_bonus_bound * (2 * relative - 1)
                )
            feature = {
                "region": region_id,
                "file": path,
                "region_width": width,
                "region_owners": owners,
                "exact_lines": exact,
                "hunks": hunks,
                "entry_count_observations": [
                    {
                        "snapshot": list(key),
                        "entry_count": value,
                        "censored": value == 254,
                    }
                    for key, value in sorted(runs.items())
                ],
                "entry_count_percentile": relative,
                "coverage_run_frequency": len(runs),
            }
            candidate = candidates.setdefault(
                test,
                {
                    "test": test,
                    "source": "primary_coverage",
                    "score": 0.0,
                    "admission_reason": "precise_exact_or_hunk_coverage",
                    "features": [],
                },
            )
            candidate["score"] += multiplier * weight / (width * owners)
            candidate["features"].append(feature)

    def order(candidate):
        legacy_tier = 0
        if entry_mode == "legacy-tier":
            legacy_tier = not any(
                observation["entry_count"] <= 10
                for feature in candidate["features"]
                for observation in feature["entry_count_observations"]
            )
        return legacy_tier, -candidate["score"], candidate["test"]

    ranked = sorted(candidates.values(), key=order)
    for rank, candidate in enumerate(ranked, 1):
        candidate["rank"] = rank
    return ranked


def protect_selection(changed, failed, candidates, normalize, config=SELECTION_CONFIG):
    records = {}
    for source, tests in (("changed", changed), ("previously_failed", failed)):
        for test in tests:
            name = normalize(test)
            if name in records:
                records[name]["sources"].append(source)
            else:
                records[name] = {
                    "test": name,
                    "source": source,
                    "sources": [source],
                    "score": None,
                    "admission_reason": "mandatory",
                    "features": [],
                }
    mandatory_count = len(records)
    rejected = []
    for candidate in candidates:
        name = normalize(candidate["test"])
        if name in records:
            records[name]["sources"].append("primary_coverage")
            records[name]["score"] = candidate["score"]
            records[name]["features"] = candidate["features"]
        elif len(records) < config.max_selected_tests_temporary:
            records[name] = {**candidate, "test": name, "sources": ["primary_coverage"]}
        else:
            rejected.append(
                {**candidate, "test": name, "admission_reason": "temporary_ceiling"}
            )
    selected = list(records.values())
    for rank, record in enumerate(selected, 1):
        record["rank"] = rank
    return {
        "selected": selected,
        "rejected": rejected,
        "selected_count": len(selected),
        "mandatory_count": mandatory_count,
        "mandatory_overflow": max(
            0, mandatory_count - config.max_selected_tests_temporary
        ),
        "ceiling_truncated": bool(rejected),
    }

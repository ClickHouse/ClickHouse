"""Coverage contract, query construction and scoring of the targeted test selection."""

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

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


def snapshot_query_settings(config=SELECTION_CONFIG):
    return (
        f"SETTINGS use_query_cache = 1, "
        f"query_cache_ttl = {config.snapshot_query_cache_ttl_sec}"
    )


def snapshot_times_query(cutoff, config=SELECTION_CONFIG):
    # Reads only `check_start_time`, which compresses to almost nothing (about
    # 0.3 s for the 14-day window). It deliberately does not filter by
    # `check_name`, which would read that column for billions of rows.
    return f"""
        SELECT DISTINCT check_start_time
        FROM checks_coverage_lines
        WHERE check_start_time <= toDateTime({sql_string(cutoff)}, 'UTC')
          AND check_start_time > toDateTime({sql_string(cutoff)}, 'UTC')
              - INTERVAL {config.coverage_search_days} DAY
        {snapshot_query_settings(config)}
        FORMAT JSONEachRow
    """


def snapshot_query(times, config=SELECTION_CONFIG):
    # Temporary identity until CIDB has a workflow run/shard metadata table.
    # Select independent observations per shard; hours are never workflow IDs.
    # The explicit timestamps let the primary key skip everything else, as
    # `uniqExact(test_name)` over the whole window reads tens of GB.
    keys = ", ".join(f"toDateTime({sql_string(t)}, 'UTC')" for t in times)
    return f"""
        SELECT check_start_time, check_name, uniqExact(test_name) AS exported_tests
        FROM checks_coverage_lines
        WHERE check_start_time IN ({keys})
          AND check_name LIKE {sql_string(config.coverage_check_name_like)}
          AND match(test_name, {sql_string(config.coverage_test_name_pattern)})
        GROUP BY check_start_time, check_name
        HAVING exported_tests >= {config.min_exported_tests_per_shard}
        ORDER BY check_start_time DESC, check_name
        {snapshot_query_settings(config)}
        FORMAT JSONEachRow
    """


def load_snapshots(query, cutoff, config=SELECTION_CONFIG):
    """Return the newest `coverage_run_count` healthy snapshots per shard.

    `query(sql, timeout)` runs SQL and returns the raw `JSONEachRow` response. Timestamps are
    walked newest first, a few at a time, until every shard seen has enough
    healthy snapshots, so only the exports actually used are read.
    """
    times = sorted(
        {row["check_start_time"] for row in parse_rows(query(snapshot_times_query(cutoff, config), config.snapshot_query_timeout_sec))},
        reverse=True,
    )
    per_shard = defaultdict(list)
    batch = config.snapshot_batch_timestamps
    for start in range(0, len(times), batch):
        for row in parse_rows(
            query(snapshot_query(times[start : start + batch], config), config.snapshot_query_timeout_sec)
        ):
            per_shard[row["check_name"]].append(row)
        if len(per_shard) >= config.coverage_shards and all(
            len(rows) >= config.coverage_run_count for rows in per_shard.values()
        ):
            break
    snapshots = [row for rows in per_shard.values() for row in rows[: config.coverage_run_count]]
    snapshots.sort(key=lambda row: row["check_name"])
    snapshots.sort(key=lambda row: row["check_start_time"], reverse=True)
    return snapshots


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


def build_selector_smoke_seed_query(source, config=SELECTION_CONFIG):
    # Choose repository source coordinates for the smoke, while preserving all
    # recorded paths in the coverage export.
    return f"""
        SELECT canonical_file AS file, line_start, line_end
        FROM
        (
            SELECT if(startsWith(file, './'), substring(file, 3), file) AS canonical_file,
                   line_start, line_end, test_name
            FROM {source}
              AND (startsWith(file, 'src/') OR startsWith(file, './src/'))
              AND match(test_name, {sql_string(config.coverage_test_name_pattern)})
        )
        GROUP BY canonical_file, line_start, line_end
        HAVING line_end >= line_start
           AND line_end - line_start + 1 <= {config.narrow_region_max_lines}
           AND uniqExact(test_name) <= {config.max_precise_region_owners}
        ORDER BY line_end - line_start, file, line_start LIMIT 1 FORMAT JSONEachRow
    """


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
              AND match(test_name, {sql_string(config.coverage_test_name_pattern)})
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


def build_bracket_spans_query(hunk_ranges, snapshots, config=SELECTION_CONFIG):
    """Regions around the changed hunks per snapshot, to find the hunks that overlap no region."""
    conditions = []
    for path, hunks in sorted(hunk_ranges.items()):
        paths = ", ".join(map(sql_string, canonical_coverage_paths(path)))
        windows = " OR ".join(
            f"(line_end >= {max(0, a - config.bracket_gap_lines)} AND line_start <= {max(a, b) + config.bracket_gap_lines})"
            for a, b in sorted(set(hunks))
        )
        conditions.append(f"(file IN ({paths}) AND ({windows}))")
    if not conditions:
        raise ValueError("Bracket query needs changed hunks")
    # `observed_at` must not be named `check_start_time`: the alias would replace the
    # column in the snapshot predicate.
    return f"""
        SELECT DISTINCT if(startsWith(file, './'), substring(file, 3), file) AS canonical_file,
               line_start, line_end, toString(check_start_time) AS observed_at, check_name
        FROM checks_coverage_lines
        WHERE {snapshot_predicate(snapshots)}
          AND match(test_name, {sql_string(config.coverage_test_name_pattern)})
          AND line_end >= line_start
          AND ({' OR '.join(conditions)})
        ORDER BY canonical_file, line_start, line_end, observed_at, check_name
        FORMAT JSONEachRow
    """


def find_brackets(hunk_ranges, spans, config=SELECTION_CONFIG):
    """For every hunk that overlaps no region, the nearest regions before and after it.

    A run that reached both regions ran the straight-line code between them, which the
    export does not record (it keeps one region per counter). The regions are paired
    within one snapshot, as the snapshots come from different commits whose lines can
    shift. Like a precise region, a pair is one piece of evidence however many hunks it
    brackets, so the result has one record per pair: `{"file", "before", "after",
    "width", "snapshots", "hunks"}`, with regions as `(file, line_start, line_end)` and
    `width` the number of lines between them.
    """
    by_file = defaultdict(lambda: defaultdict(list))
    for span in spans:
        by_file[canonical_coverage_path(span["canonical_file"])][
            (span["observed_at"], span["check_name"])
        ].append((int(span["line_start"]), int(span["line_end"])))
    brackets = {}
    for path, hunks in sorted(hunk_ranges.items()):
        path = canonical_coverage_path(path)
        snapshots = by_file.get(path, {})
        for a, b in sorted(set(hunks)):
            b = max(a, b)
            # A hunk that overlaps a region in any snapshot is scored by that region.
            if any(end >= a and start <= b for regions in snapshots.values() for start, end in regions):
                continue
            for snapshot, regions in sorted(snapshots.items()):
                before = [r for r in regions if r[1] < a and a - r[1] <= config.bracket_gap_lines]
                after = [r for r in regions if r[0] > b and r[0] - b <= config.bracket_gap_lines]
                if not before or not after:
                    continue
                before = (path, *max(before, key=lambda r: (r[1], -r[0])))
                after = (path, *min(after, key=lambda r: (r[0], r[1])))
                bracket = brackets.setdefault(
                    (before, after),
                    {
                        "file": path,
                        "before": before,
                        "after": after,
                        "width": after[1] - before[2] + 1,
                        "snapshots": [],
                        "hunks": [],
                    },
                )
                if snapshot not in bracket["snapshots"]:
                    bracket["snapshots"].append(snapshot)
                hunk = f"{path}:{a}-{b}"
                if hunk not in bracket["hunks"]:
                    bracket["hunks"].append(hunk)
    return [brackets[key] for key in sorted(brackets)]


def build_bracket_owners_query(brackets, snapshots, config=SELECTION_CONFIG):
    regions = defaultdict(set)
    for bracket in brackets:
        for path, start, end in (bracket["before"], bracket["after"]):
            regions[path].add((start, end))
    if not regions:
        raise ValueError("Bracket owners query needs regions")
    conditions = []
    for path, spans in sorted(regions.items()):
        paths = ", ".join(map(sql_string, canonical_coverage_paths(path)))
        keys = ", ".join(f"({start}, {end})" for start, end in sorted(spans))
        conditions.append(f"(file IN ({paths}) AND (line_start, line_end) IN ({keys}))")
    return f"""
        SELECT if(startsWith(file, './'), substring(file, 3), file) AS canonical_file,
               line_start, line_end, toString(check_start_time) AS observed_at, check_name,
               groupUniqArray(test_name) AS owners
        FROM checks_coverage_lines
        WHERE {snapshot_predicate(snapshots)}
          AND match(test_name, {sql_string(config.coverage_test_name_pattern)})
          AND ({' OR '.join(conditions)})
        GROUP BY canonical_file, line_start, line_end, observed_at, check_name
        ORDER BY canonical_file, line_start, line_end, observed_at, check_name
        FORMAT JSONEachRow
    """


def attach_bracket_owners(brackets, rows):
    """Set `owners` of every bracket to the tests that own both of its regions in one
    of the snapshots where they are paired."""
    owners = defaultdict(set)
    for row in rows:
        path = canonical_coverage_path(row["canonical_file"])
        snapshot = (row["observed_at"], row["check_name"])
        owners[(path, int(row["line_start"]), int(row["line_end"]), snapshot)].update(row["owners"])
    for bracket in brackets:
        found = set()
        for snapshot in bracket["snapshots"]:
            found |= owners[(*bracket["before"], snapshot)] & owners[(*bracket["after"], snapshot)]
        bracket["owners"] = sorted(found)
    return brackets


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
    brackets=None,
):
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
        for test, runs in sorted(observations.items()):
            feature = {
                "region": region_id,
                "file": path,
                "region_width": width,
                "region_owners": owners,
                "exact_lines": exact,
                "hunks": hunks,
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
            candidate["score"] += weight / (width * owners)
            candidate["features"].append(feature)

    for bracket in brackets or []:
        owners = len(bracket["owners"])
        if not 0 < owners <= config.max_precise_region_owners:
            continue
        width = bracket["width"]
        for test in bracket["owners"]:
            feature = {
                "region": f"{bracket['file']}:{bracket['before'][2]}-{bracket['after'][1]}",
                "file": bracket["file"],
                "region_width": width,
                "region_owners": owners,
                "exact_lines": [],
                "hunks": bracket["hunks"],
                "bracket_regions": [
                    f"{path}:{start}-{end}" for path, start, end in (bracket["before"], bracket["after"])
                ],
                # The owners query does not count the runs per test.
                "coverage_run_frequency": None,
            }
            candidate = candidates.setdefault(
                test,
                {
                    "test": test,
                    "source": "primary_coverage",
                    "score": 0.0,
                    "admission_reason": "bracketed_hunk_coverage",
                    "features": [],
                },
            )
            candidate["score"] += config.hunk_context_weight / (width * owners)
            candidate["features"].append(feature)

    ranked = sorted(
        candidates.values(), key=lambda candidate: (-candidate["score"], candidate["test"])
    )
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

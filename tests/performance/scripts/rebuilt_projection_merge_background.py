#!/usr/bin/env python3
"""Measure rebuilt projection background merges."""

import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import threading
import time


# Sample only while the isolated server executes the scheduled merge. The file
# descriptor counters deliberately include the parent-part I/O as well as legacy
# temporary projection parts or streamed `Native` runs, so remerge I/O cannot
# disappear behind projection-part-only counters. The separate projection-part
# and external-sort counters identify the portions written by those paths.
PROJECTION_SERIALIZATION_EVENT = "MergeTreeProjectionSerializationCompressedBytes"
LEGACY_PROJECTION_SERIALIZATION_EVENT = "MergeTreeRebuiltProjectionCompressedBytes"
RECURSIVE_PROJECTION_PART_MERGE_COMMITS_EVENT = "MergeTreeRecursiveProjectionPartMergeCommits"
LEGACY_PROJECTION_PART_COMMITS_EVENT = "MergeTreeRebuiltProjectionPartCommits"
PROFILE_EVENT_ALIASES = (
    (PROJECTION_SERIALIZATION_EVENT, LEGACY_PROJECTION_SERIALIZATION_EVENT),
    (RECURSIVE_PROJECTION_PART_MERGE_COMMITS_EVENT, LEGACY_PROJECTION_PART_COMMITS_EVENT),
)
EVENTS = (
    "MergeMutateBackgroundExecutorTaskExecuteStepMicroseconds",
    "UserTimeMicroseconds",
    "SystemTimeMicroseconds",
    "ReadBufferFromFileDescriptorReadBytes",
    "WriteBufferFromFileDescriptorWriteBytes",
    "MergeTreeDataProjectionWriterBlocks",
    "MergeTreeDataProjectionWriterCompressedBytes",
    PROJECTION_SERIALIZATION_EVENT,
    LEGACY_PROJECTION_SERIALIZATION_EVENT,
    RECURSIVE_PROJECTION_PART_MERGE_COMMITS_EVENT,
    LEGACY_PROJECTION_PART_COMMITS_EVENT,
    "MergeTreeDataProjectionWriterFinalParts",
    "MergeVerticalStageExecuteMilliseconds",
    "ExternalSortCompressedBytes",
    "ExternalSortWritePart",
    "MergedProjections",
    "RebuiltProjections",
)
SOURCE_PARTS = 12
LEGACY_PROJECTION_MERGE_FAN_IN = 10
BACKGROUND_SQUASH_ROWS = 65_536
WIDE_PART_ROWS = 2_000_000
INDEX_ROWS_PER_PART = 25_000
ROW_REDUCING_WIDE_PART_ROWS = 500_000
DEFAULT_VERTICAL_NON_KEY_COLUMNS = 11
SYNC_MERGE_TIMEOUT_SECONDS = 120
SCENARIOS = ("normal", "aggregate", "reordered", "reordered_aggregate")
REDUCING_SCENARIOS = SCENARIOS + ("parent_offset",)
VERTICAL_SCENARIOS = ("normal", "aggregate", "reordered", "reordered_aggregate")
DEFAULT_VERTICAL_SCENARIOS = ("normal", "aggregate", "reordered", "reordered_aggregate")
ROW_REDUCING_WIDE_SCENARIOS = ("normal",)
ROW_REDUCING_DEFAULT_FORMAT_SCENARIOS = ("normal",)
EXPIRED_SOURCE_SCENARIOS = ("normal",)
ROW_REDUCING_VERTICAL_SCENARIOS = ("normal", "aggregate", "reordered", "reordered_aggregate")
FILTERED_COMPACT_SCENARIOS = ("filtered", "filtered_reordered")
FILTERED_SCENARIOS = FILTERED_COMPACT_SCENARIOS + ("filtered_wide",)
DEFAULT_FORMAT_SCENARIOS = ("normal",)
DEFAULT_BLOCKS_SCENARIOS = ("normal",)
INDEX_SCENARIOS = ("index",)
DYNAMIC_SCENARIOS = ("dynamic",)
COMMIT_ORDER_SCENARIOS = ("commit_order",)
MULTI_PROJECTION_SCENARIOS = SCENARIOS
FILTERED_KEY_LIMIT = 4
FILTERED_WIDE_KEY_LIMIT = 40_000
AGGREGATE_SCENARIOS = frozenset(("aggregate", "reordered_aggregate"))
ORDINARY_AGGREGATE_REPETITIONS = 10
REDUCING_BATCH_SCENARIOS = frozenset(("aggregate", "reordered", "reordered_aggregate", "parent_offset"))
REDUCING_BATCH_REPETITIONS = 2
ORDINARY_PART_TYPES = {
    "normal": "Wide",
    "aggregate": "Compact",
    "reordered": "Wide",
    "reordered_aggregate": "Compact",
}
COMPACT_PART_TYPES = dict.fromkeys(REDUCING_SCENARIOS, "Compact")
WORKLOADS = {
    "ordinary": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "vertical_mode": "disabled",
        "wide_part_rows": WIDE_PART_ROWS,
        "expected_projection_part_types": ORDINARY_PART_TYPES,
        "scenarios": SCENARIOS,
    },
    "reducing": {
        "projection_groups": 1_000_000,
        "rows_per_part": 1_000_000,
        "row_reducing": True,
        "vertical_mode": "disabled",
        "wide_part_rows": WIDE_PART_ROWS,
        "expected_projection_part_types": COMPACT_PART_TYPES,
        "scenarios": REDUCING_SCENARIOS,
    },
    "vertical": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "vertical_mode": "forced",
        "wide_part_rows": 1,
        "expected_projection_part_types": dict.fromkeys(VERTICAL_SCENARIOS, "Wide"),
        "scenarios": VERTICAL_SCENARIOS,
    },
    "vertical_default": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "vertical_mode": "default",
        "wide_part_rows": WIDE_PART_ROWS,
        "expected_projection_part_types": ORDINARY_PART_TYPES,
        "scenarios": DEFAULT_VERTICAL_SCENARIOS,
    },
    "reducing_wide": {
        "projection_groups": 1_000_000,
        "rows_per_part": 1_000_000,
        "row_reducing": True,
        "vertical_mode": "disabled",
        "wide_part_rows": ROW_REDUCING_WIDE_PART_ROWS,
        "expected_projection_part_types": {"normal": "Wide"},
        "scenarios": ROW_REDUCING_WIDE_SCENARIOS,
    },
    # The reducing writer must make its delayed choice with the repository's
    # actual format thresholds, rather than only forced Compact or Wide ones.
    "reducing_default_format": {
        "projection_groups": 1_000_000,
        "rows_per_part": 1_000_000,
        "row_reducing": True,
        "vertical_mode": "disabled",
        "wide_part_rows": None,
        "expected_projection_part_types": {"normal": "Wide"},
        "scenarios": ROW_REDUCING_DEFAULT_FORMAT_SCENARIOS,
    },
    "expired_source": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "vertical_mode": "disabled",
        "wide_part_rows": 1,
        "expected_projection_part_types": {"normal": "Wide"},
        "scenarios": EXPIRED_SOURCE_SCENARIOS,
        "expired_source_column": True,
    },
    "reducing_vertical": {
        "projection_groups": 250_000,
        "rows_per_part": 250_000,
        "row_reducing": True,
        "vertical_mode": "forced",
        "wide_part_rows": 1,
        "expected_projection_part_types": dict.fromkeys(ROW_REDUCING_VERTICAL_SCENARIOS, "Wide"),
        "scenarios": ROW_REDUCING_VERTICAL_SCENARIOS,
    },
    # The first parent block is fully retained, then the filter discards the
    # remaining input. This mode deliberately uses a one-row background squash.
    # With one-row post-projection squashing, this exercises
    # the delayed format choice using the engine's unmodified defaults.
    "filtered": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "vertical_mode": "disabled",
        "wide_part_rows": None,
        "expected_projection_part_types": dict.fromkeys(FILTERED_COMPACT_SCENARIOS, "Compact"),
        "scenarios": FILTERED_COMPACT_SCENARIOS,
        "background_squash_rows": 1,
    },
    # The same filtered rebuild can cross the Wide threshold after retaining a material fraction of its input.
    "filtered_wide": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "vertical_mode": "disabled",
        "wide_part_rows": 500_000,
        "expected_projection_part_types": {"filtered_wide": "Wide"},
        "scenarios": ("filtered_wide",),
        "background_squash_rows": 1,
    },
    # A wide normal projection under the same unmodified format defaults.
    "default_format": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "vertical_mode": "disabled",
        "wide_part_rows": None,
        "expected_projection_part_types": {"normal": "Wide"},
        "scenarios": DEFAULT_FORMAT_SCENARIOS,
    },
    "index": {
        "projection_groups": 65_536,
        # One mark per row is intentional for the index probe, but keeping its
        # fixed input below the full matrix size avoids turning index materialization
        # into a timeout rather than a rebuilt-projection measurement.
        "rows_per_part": INDEX_ROWS_PER_PART,
        "row_reducing": False,
        "vertical_mode": "disabled",
        "wide_part_rows": 1,
        "expected_projection_part_types": {"index": "Wide"},
        "scenarios": INDEX_SCENARIOS,
    },
    "dynamic": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "vertical_mode": "disabled",
        "wide_part_rows": WIDE_PART_ROWS,
        "expected_projection_part_types": {"dynamic": "Wide"},
        "scenarios": DYNAMIC_SCENARIOS,
    },
    "optimize_row_order": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "vertical_mode": "disabled",
        "wide_part_rows": WIDE_PART_ROWS,
        "expected_projection_part_types": {"normal": "Wide"},
        "scenarios": ("normal",),
        "optimize_row_order": True,
    },
    "commit_order": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "vertical_mode": "disabled",
        "wide_part_rows": WIDE_PART_ROWS,
        "expected_projection_part_types": {"commit_order": "Wide"},
        "scenarios": COMMIT_ORDER_SCENARIOS,
    },
    "multi_projection": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "vertical_mode": "disabled",
        "wide_part_rows": WIDE_PART_ROWS,
        "expected_projection_part_types": ORDINARY_PART_TYPES,
        "scenarios": ("multi",),
    },
    "summing": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "summing_merge": True,
        "vertical_mode": "disabled",
        "wide_part_rows": 1,
        "expected_projection_part_types": {"normal": "Wide"},
        "scenarios": ("normal",),
    },
    "row_ttl": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "row_ttl": True,
        "vertical_mode": "disabled",
        "wide_part_rows": 1,
        "expected_projection_part_types": {"normal": "Wide"},
        "scenarios": ("normal",),
    },
    # Keep both merge block controls at their repository defaults.
    "default_blocks": {
        "projection_groups": 65_536,
        "rows_per_part": 250_000,
        "row_reducing": False,
        "vertical_mode": "disabled",
        "wide_part_rows": None,
        "expected_projection_part_types": {"normal": "Wide"},
        "scenarios": DEFAULT_BLOCKS_SCENARIOS,
        "merge_max_block_size": None,
        "background_squash_rows": None,
        "use_default_selector": True,
    },
}
METRIC_SUFFIXES = (
    "merge_seconds",
    "cpu_seconds",
    "executor_step_seconds",
    "merge_file_read_compressed_bytes",
    "merge_file_write_compressed_bytes",
    "projection_serialization_compressed_bytes",
    "external_sort_compressed_bytes",
    "temporary_projection_part_writes",
    "temporary_projection_part_commits",
    "peak_rss_kib",
)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("clickhouse", type=Path)
    parser.add_argument("source_root", type=Path)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--mode", choices=tuple(WORKLOADS), required=True)
    parser.add_argument(
        "--aggregate-metrics",
        action="store_true",
        help=(
            "report one fixed-workload aggregate for each metric instead of scenario rows; "
            "ordinary aggregate scenarios run ten times and reducing aggregate/reordered scenarios run twice"
        ),
    )
    return parser.parse_args()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def query(binary, port, sql, multiquery=False, timeout=None):
    command = [str(binary), "client", "--host", "127.0.0.1", "--port", str(port)]
    if multiquery:
        command.append("--multiquery")
    try:
        result = subprocess.run(command + ["--query", sql], text=True, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired as ex:
        raise RuntimeError(f"query timed out after {timeout} seconds: {sql}") from ex
    if result.returncode:
        raise RuntimeError(f"query failed ({result.returncode}): {sql}\n{result.stderr}")
    return result.stdout


def process_rss_kib(pid):
    for line in Path(f"/proc/{pid}/status").read_text(encoding="ascii").splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    return 0


def profile_events(binary, port):
    values = dict.fromkeys(EVENTS, 0)
    present = set()
    names = ",".join(repr(event) for event in EVENTS)
    for line in query(binary, port, f"SELECT event, value FROM system.events WHERE event IN ({names}) FORMAT TabSeparatedRaw").splitlines():
        event, value = line.split("\t")
        values[event] = int(value)
        present.add(event)

    # The comparison binary predates the corrected event names. Resolve each
    # pair by availability so an absent event cannot look like a real zero.
    for current_event, legacy_event in PROFILE_EVENT_ALIASES:
        current_present = current_event in present
        legacy_present = legacy_event in present
        if current_present == legacy_present:
            names = f"{current_event!r}, {legacy_event!r}"
            state = "both present" if current_present else "neither present"
            raise RuntimeError(f"expected exactly one event alias ({names}): {state}")
        value = values[current_event] if current_present else values[legacy_event]
        values[current_event] = value
        values[legacy_event] = value
    return values


def prepare_server_config(source_root, work, background_squash_rows):
    """Optionally give background merges a smaller squash boundary without fragmenting inserts."""
    source_config_dir = source_root / "programs/server"
    shutil.copyfile(source_config_dir / "config.xml", work / "config.xml")
    shutil.copytree(source_config_dir / "config.d", work / "config.d")
    if background_squash_rows is None:
        shutil.copyfile(source_config_dir / "users.xml", work / "users.xml")
        return work / "config.xml"

    users = (source_config_dir / "users.xml").read_text(encoding="utf-8")
    marker = """        <!-- Settings for background operations (e.g. Merge, Mutate) -->
        <!-- <background> -->
        <!-- </background> -->"""
    replacement = f"""        <!-- Settings for background operations (e.g. Merge, Mutate) -->
        <background>
            <min_insert_block_size_rows>{background_squash_rows}</min_insert_block_size_rows>
            <min_insert_block_size_bytes>0</min_insert_block_size_bytes>
        </background>"""
    if users.count(marker) != 1:
        raise RuntimeError("could not install the background merge profile")
    (work / "users.xml").write_text(users.replace(marker, replacement), encoding="utf-8")
    return work / "config.xml"


def projection_definition(scenario, projection_groups):
    if scenario == "normal":
        return "p_normal", "PROJECTION p_normal (SELECT k, v ORDER BY k)"
    if scenario == "aggregate":
        return "p_aggregate", "PROJECTION p_aggregate (SELECT k, sum(v) GROUP BY k)"
    if scenario == "reordered":
        return "p_reordered", "PROJECTION p_reordered (SELECT v, k ORDER BY v)"
    if scenario == "reordered_aggregate":
        return (
            "p_reordered_aggregate",
            f"PROJECTION p_reordered_aggregate (SELECT (k * 7) % {projection_groups} AS g, sum(v) GROUP BY g)",
        )
    if scenario == "parent_offset":
        return "p_parent_offset", "PROJECTION p_parent_offset (SELECT k, v, _part_offset ORDER BY v)"
    if scenario == "commit_order":
        return (
            "_commit_order",
            "PROJECTION _commit_order (SELECT *, _block_number, _block_offset ORDER BY _block_number, _block_offset)",
        )
    if scenario == "multi":
        projections = ",\n    ".join(projection_definition(name, projection_groups)[1] for name in MULTI_PROJECTION_SCENARIOS)
        return "p_multi", projections
    if scenario == "filtered":
        return "p_filtered", f"PROJECTION p_filtered (SELECT k, v WHERE k < {FILTERED_KEY_LIMIT} ORDER BY k)"
    if scenario == "filtered_reordered":
        return "p_filtered_reordered", f"PROJECTION p_filtered_reordered (SELECT k, v WHERE k < {FILTERED_KEY_LIMIT} ORDER BY v)"
    if scenario == "filtered_wide":
        return "p_filtered_wide", f"PROJECTION p_filtered_wide (SELECT k, v WHERE k < {FILTERED_WIDE_KEY_LIMIT} ORDER BY k)"
    if scenario == "index":
        return "p_index", "PROJECTION p_index INDEX v TYPE basic"
    if scenario == "dynamic":
        # Keep the JSON cast as a materialized projection expression rather than
        # allowing it to collapse back to the source String column.
        return "p_dynamic", "PROJECTION p_dynamic (SELECT k, CAST(concat(raw, ''), 'JSON(max_dynamic_paths=4)') AS json WHERE k >= 0 ORDER BY k)"
    raise ValueError(f"unknown scenario: {scenario}")


def projection_filter(scenario):
    if scenario in {"filtered", "filtered_reordered"}:
        return f" WHERE k < {FILTERED_KEY_LIMIT}"
    if scenario == "filtered_wide":
        return f" WHERE k < {FILTERED_WIDE_KEY_LIMIT}"
    return ""


def vertical_extra_columns(vertical_mode):
    if vertical_mode == "forced":
        return ("extra",)
    if vertical_mode == "default":
        return tuple(f"extra{index}" for index in range(DEFAULT_VERTICAL_NON_KEY_COLUMNS))
    if vertical_mode == "disabled":
        return ()
    raise ValueError(f"unknown vertical mode: {vertical_mode}")


def require_matching_row_multisets(binary, port, projection_name, filter_clause=""):
    """Compare every projected (k, v) multiplicity without allowing duplicate rows to mask a mismatch."""
    mismatches = query(
        binary,
        port,
        f"""
SELECT count()
FROM
(
    SELECT k, v, count() AS copies
    FROM bench.t{filter_clause}
    GROUP BY k, v
) AS parent
FULL OUTER JOIN
(
    SELECT k, v, count() AS copies
    FROM mergeTreeProjection('bench', 't', '{projection_name}')
    GROUP BY k, v
) AS projection USING (k, v)
WHERE isNull(parent.copies) OR isNull(projection.copies) OR parent.copies != projection.copies
SETTINGS join_use_nulls = 1, optimize_use_projections = 0
""",
    ).strip()
    if mismatches != "0":
        raise RuntimeError(f"projection row multiset differs for {projection_name}: {mismatches!r}")


def require_matching_aggregate_groups(binary, port, projection_name, projection_groups, reordered):
    """Compare every aggregate key and finalized aggregate value, including missing groups."""
    if reordered:
        parent_key = f"(k * 7) % {projection_groups}"
    else:
        parent_key = "k"
    schema = query(
        binary,
        port,
        f"DESCRIBE TABLE mergeTreeProjection('bench', 't', '{projection_name}') FORMAT TabSeparatedRaw",
    ).splitlines()
    group_columns = [
        line.split("\t", 2)[0]
        for line in schema
        if len(line.split("\t", 2)) > 1 and not line.split("\t", 2)[1].startswith("AggregateFunction")
    ]
    if len(group_columns) != 1:
        raise RuntimeError(f"aggregate projection did not expose one group key: {schema!r}")
    projection_key = f"`{group_columns[0].replace('`', '``')}`"
    mismatches = query(
        binary,
        port,
        f"""
SELECT count()
FROM
(
    SELECT {parent_key} AS group_key, sum(v) AS value
    FROM bench.t
    GROUP BY group_key
) AS parent
FULL OUTER JOIN
(
    SELECT {projection_key} AS group_key, sumMerge(`sum(v)`) AS value
    FROM mergeTreeProjection('bench', 't', '{projection_name}')
    GROUP BY group_key
) AS projection USING (group_key)
WHERE isNull(parent.value) OR isNull(projection.value) OR parent.value != projection.value
SETTINGS join_use_nulls = 1, optimize_use_projections = 0
""",
    ).strip()
    if mismatches != "0":
        raise RuntimeError(f"projection aggregate groups differ for {projection_name}: {mismatches!r}")


def validate_multi_projection(binary, port, projection_groups, rows_per_part, expected_projection_part_types):
    checks = query(
        binary,
        port,
        """
SELECT count(), sum(v), sum(k) FROM bench.t;
SELECT count(), sum(v), sum(k) FROM mergeTreeProjection('bench', 't', 'p_normal');
SELECT count(), sumMerge(`sum(v)`) FROM mergeTreeProjection('bench', 't', 'p_aggregate');
SELECT count(), sum(v), sum(k) FROM mergeTreeProjection('bench', 't', 'p_reordered');
SELECT count(), sumMerge(`sum(v)`) FROM mergeTreeProjection('bench', 't', 'p_reordered_aggregate');
SELECT count() FROM system.parts WHERE database = 'bench' AND table = 't' AND active;
SELECT count() FROM system.projection_parts WHERE database = 'bench' AND table = 't' AND active;
CHECK TABLE bench.t SETTINGS check_query_single_value_result = 1;
""",
        multiquery=True,
    ).splitlines()
    total_rows = rows_per_part * SOURCE_PARTS
    if len(checks) != 8 or not checks[0].startswith(f"{total_rows}\t"):
        raise RuntimeError(f"multi-projection parent validation failed: {checks!r}")
    if checks[1] != checks[0] or checks[3] != checks[0]:
        raise RuntimeError(f"multi-projection ordered output validation failed: {checks!r}")
    parent_sum = checks[0].split("\t")[1]
    for aggregate_result in (checks[2], checks[4]):
        aggregate_rows, aggregate_sum = aggregate_result.split("\t")
        if aggregate_rows != str(projection_groups) or aggregate_sum != parent_sum:
            raise RuntimeError(f"multi-projection aggregate validation failed: {checks!r}")
    if checks[5:] != ["1", str(len(MULTI_PROJECTION_SCENARIOS)), "1"]:
        raise RuntimeError(f"multi-projection part validation failed: {checks!r}")

    require_matching_row_multisets(binary, port, "p_normal")
    require_matching_row_multisets(binary, port, "p_reordered")
    require_matching_aggregate_groups(binary, port, "p_aggregate", projection_groups, reordered=False)
    require_matching_aggregate_groups(binary, port, "p_reordered_aggregate", projection_groups, reordered=True)

    part_types = dict(
        line.split("\t", 1)
        for line in query(
            binary,
            port,
            "SELECT name, part_type FROM system.projection_parts "
            "WHERE database = 'bench' AND table = 't' AND active ORDER BY name FORMAT TabSeparatedRaw",
        ).splitlines()
    )
    expected = {
        projection_definition(name, projection_groups)[0]: expected_projection_part_types[name]
        for name in MULTI_PROJECTION_SCENARIOS
    }
    if part_types != expected:
        raise RuntimeError(f"multi-projection part formats differ: {part_types!r}")


def validate_merge(
    binary,
    port,
    projection_groups,
    rows_per_part,
    scenario,
    row_reducing,
    summing_merge,
    row_ttl,
    vertical_mode,
    expired_source_column,
    expected_projection_part_type,
):
    if scenario == "multi":
        validate_multi_projection(binary, port, projection_groups, rows_per_part, expected_projection_part_type)
        return

    projection_name, _ = projection_definition(scenario, projection_groups)
    aggregate_projection = scenario in {"aggregate", "reordered_aggregate"}
    filter_clause = projection_filter(scenario)
    representation_projection = scenario in {"index", "dynamic"}
    parent_offset_projection = scenario == "parent_offset"
    if aggregate_projection:
        projection_query = f"SELECT count(), sumMerge(`sum(v)`) FROM mergeTreeProjection('bench', 't', '{projection_name}')"
    elif representation_projection:
        projection_query = f"SELECT count() FROM mergeTreeProjection('bench', 't', '{projection_name}')"
    else:
        projection_query = f"SELECT count(), sum(v), sum(k) FROM mergeTreeProjection('bench', 't', '{projection_name}')"

    checks = query(
        binary,
        port,
        f"""
SELECT count(), sum(v), sum(k) FROM bench.t;
SELECT count(), sum(v), sum(k) FROM bench.t{filter_clause};
{projection_query};
SELECT count() FROM system.parts WHERE database = 'bench' AND table = 't' AND active;
SELECT count() FROM system.projection_parts WHERE database = 'bench' AND table = 't' AND active;
CHECK TABLE bench.t SETTINGS check_query_single_value_result = 1;
""",
        multiquery=True,
    ).splitlines()

    total_rows = rows_per_part * SOURCE_PARTS
    if len(checks) != 6:
        raise RuntimeError(f"projection validation failed: {checks!r}")

    if row_reducing:
        expected_key_sum = projection_groups * (projection_groups - 1) // 2
        expected_value_sum = expected_key_sum + projection_groups * (SOURCE_PARTS - 1) * rows_per_part
        expected_parent = f"{projection_groups}\t{expected_value_sum}\t{expected_key_sum}"
        expected_extra_sum = expected_value_sum
        parent_valid = checks[0] == expected_parent and projection_groups < total_rows
    elif summing_merge:
        expected_extra_sum = 0
        parent_valid = checks[0].startswith(f"{projection_groups}\t") and projection_groups < total_rows
    elif row_ttl:
        expected_extra_sum = 0
        parent_valid = checks[0].startswith(f"{total_rows // 2}\t")
    else:
        expected_parent = checks[0]
        expected_extra_sum = total_rows * (total_rows - 1) // 2
        parent_valid = checks[0].startswith(f"{total_rows}\t")

    if scenario in FILTERED_SCENARIOS:
        filter_key_limit = FILTERED_WIDE_KEY_LIMIT if scenario == "filtered_wide" else FILTERED_KEY_LIMIT
        rows_per_source_part = (rows_per_part // projection_groups) * filter_key_limit + min(
            rows_per_part % projection_groups, filter_key_limit
        )
        expected_filtered_rows = SOURCE_PARTS * rows_per_source_part
        parent_valid = parent_valid and checks[1].startswith(f"{expected_filtered_rows}\t")

    if aggregate_projection:
        aggregate_rows, aggregate_sum = checks[2].split("\t")
        valid = aggregate_rows == str(projection_groups) and aggregate_sum == checks[1].split("\t")[1]
    elif representation_projection:
        valid = checks[2] == str(total_rows)
    else:
        valid = checks[2] == checks[1]

    if not parent_valid or not valid or checks[3:] != ["1", "1", "1"]:
        raise RuntimeError(f"projection validation failed: {checks!r}")

    if aggregate_projection:
        require_matching_aggregate_groups(
            binary, port, projection_name, projection_groups, reordered=scenario == "reordered_aggregate"
        )
    elif not representation_projection:
        require_matching_row_multisets(binary, port, projection_name, filter_clause)

    if parent_offset_projection:
        offsets = query(
            binary,
            port,
            "SELECT count(), sum(l._part_offset = r._parent_part_offset) "
            "FROM bench.t AS l JOIN mergeTreeProjection('bench', 't', 'p_parent_offset') AS r USING (k) "
            "SETTINGS enable_analyzer = 1",
        ).strip()
        expected_offsets = f"{projection_groups}\t{projection_groups}"
        if offsets != expected_offsets:
            raise RuntimeError(f"parent-offset projection validation failed: {offsets!r}")

    if scenario == "dynamic":
        schema = query(
            binary,
            port,
            "DESCRIBE TABLE mergeTreeProjection('bench', 't', 'p_dynamic') FORMAT TabSeparatedRaw",
        ).splitlines()
        json_columns = [line.split("\t", 2)[0] for line in schema if len(line.split("\t", 2)) > 1 and line.split("\t", 2)[1].startswith("JSON")]
        if len(json_columns) != 1:
            raise RuntimeError(f"dynamic projection did not expose one JSON column: {schema!r}")
        json_identifier = json_columns[0].replace("`", "``")
        paths = query(
            binary,
            port,
            f"SELECT JSONDynamicPaths(`{json_identifier}`), JSONSharedDataPaths(`{json_identifier}`) "
            "FROM mergeTreeProjection('bench', 't', 'p_dynamic') LIMIT 1",
        ).strip()
        if paths != "['a','b','c','d']\t['e','f']":
            raise RuntimeError(f"dynamic projection paths were not preserved: {paths!r}")
        aliases = tuple(f"path_{name}" for name in "abcdef")
        parent_values = ", ".join(
            f"JSONExtractUInt(raw, '{name}') AS {alias}" for name, alias in zip("abcdef", aliases)
        )
        projection_values = ", ".join(
            f"JSONExtractUInt(toJSONString(`{json_identifier}`), '{name}') AS {alias}" for name, alias in zip("abcdef", aliases)
        )
        dynamic_columns = ", ".join(("k", *aliases))
        dynamic_mismatches = query(
            binary,
            port,
            f"""
SELECT count()
FROM
(
    SELECT {dynamic_columns}, count() AS copies
    FROM (SELECT k, {parent_values} FROM bench.t)
    GROUP BY {dynamic_columns}
) AS parent
FULL OUTER JOIN
(
    SELECT {dynamic_columns}, count() AS copies
    FROM (SELECT k, {projection_values} FROM mergeTreeProjection('bench', 't', 'p_dynamic'))
    GROUP BY {dynamic_columns}
) AS projection USING ({dynamic_columns})
WHERE isNull(parent.copies) OR isNull(projection.copies) OR parent.copies != projection.copies
SETTINGS join_use_nulls = 1, optimize_use_projections = 0
""",
        ).strip()
        if dynamic_mismatches != "0":
            raise RuntimeError(f"dynamic projection values differ: {dynamic_mismatches!r}")

    if scenario == "index":
        index_results = query(
            binary,
            port,
            """
SELECT count(), sum(k) FROM bench.t WHERE v = cityHash64(0) SETTINGS optimize_use_projections = 0;
SET optimize_use_projections = 1, optimize_use_projection_filtering = 1, min_table_rows_to_use_projection_index = 0;
SELECT count(), sum(k) FROM bench.t WHERE v = cityHash64(0);
""",
            multiquery=True,
        ).splitlines()
        if index_results != ["1\t0", "1\t0"]:
            raise RuntimeError(f"projection index query results differ: {index_results!r}")

    extra_columns = vertical_extra_columns(vertical_mode)
    if extra_columns:
        extra_sums = query(
            binary,
            port,
            f"SELECT {', '.join(f'sum({column})' for column in extra_columns)} FROM bench.t",
        ).strip().split("\t")
        if extra_sums != [str(expected_extra_sum)] * len(extra_columns):
            raise RuntimeError(f"vertical merge lost gathered values: {extra_sums!r}")

    if expired_source_column:
        expired_values = query(binary, port, "SELECT min(v), max(v), countIf(v != 0) FROM bench.t").strip()
        if expired_values != "0\t0\t0":
            raise RuntimeError(f"expired source column was not read as its type default: {expired_values!r}")

    part_types = dict(
        line.split("\t", 1)
        for line in query(
            binary,
            port,
            "SELECT name, part_type FROM system.projection_parts "
            "WHERE database = 'bench' AND table = 't' AND active ORDER BY name FORMAT TabSeparatedRaw",
        ).splitlines()
    )
    if expected_projection_part_type not in {"Wide", "Compact"}:
        raise RuntimeError(f"unexpected expected projection part format: {expected_projection_part_type!r}")
    if set(part_types) != {projection_name} or set(part_types.values()) != {expected_projection_part_type}:
        raise RuntimeError(f"projection did not use its expected part format: {part_types!r}")


def active_part_names(binary, port):
    return query(
        binary,
        port,
        """
SELECT concat(char(39), name, char(39))
FROM system.parts
WHERE database = 'bench' AND table = 't' AND active
ORDER BY min_block_number
FORMAT TabSeparatedRaw
""",
    ).splitlines()


def sync_parts_merge(binary, port, parts):
    query(binary, port, f"SYSTEM SCHEDULE MERGE bench.t PARTS {','.join(parts)}")
    query(
        binary,
        port,
        f"SET max_execution_time = {SYNC_MERGE_TIMEOUT_SECONDS}; SYSTEM SYNC MERGES bench.t",
        multiquery=True,
        timeout=SYNC_MERGE_TIMEOUT_SECONDS + 10,
    )


def sync_default_selector_merges(binary, port):
    """Wait for the automatic selector without using the manual-only SYNC MERGES command."""
    query(binary, port, "SYSTEM START MERGES bench.t")
    deadline = time.monotonic() + SYNC_MERGE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        active_parts = int(
            query(
                binary,
                port,
                "SELECT count() FROM system.parts WHERE database = 'bench' AND table = 't' AND active",
            ).strip()
        )
        active_merges = int(
            query(
                binary,
                port,
                "SELECT count() FROM system.merges WHERE database = 'bench' AND table = 't'",
            ).strip()
        )
        if active_parts == 1 and active_merges == 0:
            return
        time.sleep(0.05)
    raise RuntimeError("automatic selector did not finish the bench.t merge before the timeout")


def prepare_expired_source_parts(binary, port):
    """Create projected source parts whose parent `v` data was removed by a column TTL."""
    raw_parts = active_part_names(binary, port)
    if len(raw_parts) != SOURCE_PARTS * 2:
        raise RuntimeError(f"expected {SOURCE_PARTS * 2} raw TTL parts, got {raw_parts!r}")
    if query(binary, port, "SELECT count() FROM system.projection_parts WHERE database = 'bench' AND table = 't' AND active").strip() != str(SOURCE_PARTS * 2):
        raise RuntimeError("raw TTL parts unexpectedly lack projections")

    # Each pair applies the expired `v` column TTL and rebuilds its projection.
    # The final measured merge then sees only projected source parts whose parents
    # have no physical `v` column, after the TTL definition is removed.
    for index in range(0, len(raw_parts), 2):
        sync_parts_merge(binary, port, raw_parts[index : index + 2])

    parts = active_part_names(binary, port)
    if len(parts) != SOURCE_PARTS:
        raise RuntimeError(f"expected {SOURCE_PARTS} TTL-prepared source parts, got {parts!r}")
    if query(binary, port, "SELECT count() FROM system.projection_parts WHERE database = 'bench' AND table = 't' AND active").strip() != str(SOURCE_PARTS):
        raise RuntimeError("TTL-prepared source parts unexpectedly lack projections")

    query(binary, port, "ALTER TABLE bench.t MODIFY COLUMN v REMOVE TTL")
    if "TTL" in query(binary, port, "SHOW CREATE TABLE bench.t").upper():
        raise RuntimeError("TTL metadata remained enabled for the measured merge")
    parent_v_columns = query(
        binary,
        port,
        "SELECT count() FROM system.parts_columns WHERE database = 'bench' AND table = 't' AND active AND column = 'v'",
    ).strip()
    if parent_v_columns != "0":
        raise RuntimeError(f"TTL-prepared parent parts still contain v: {parent_v_columns!r}")
    parts = active_part_names(binary, port)
    if len(parts) != SOURCE_PARTS:
        raise RuntimeError(f"TTL metadata alter changed the prepared source parts: {parts!r}")
    if query(binary, port, "SELECT count() FROM system.projection_parts WHERE database = 'bench' AND table = 't' AND active").strip() != str(SOURCE_PARTS):
        raise RuntimeError("TTL metadata alter dropped prepared source projections")
    return parts


def create_table(
    binary,
    port,
    projection_groups,
    rows_per_part,
    scenario,
    row_reducing,
    vertical_mode,
    wide_part_rows,
    expired_source_column,
    summing_merge,
    row_ttl,
    merge_max_block_size,
    use_default_selector,
    optimize_row_order,
):
    _, projection = projection_definition(scenario, projection_groups)
    table_ttl = ""

    if expired_source_column:
        if row_reducing or scenario != "normal":
            raise ValueError("expired-source workload supports only the ordinary normal projection")
        columns = "k UInt64,\n    d Date,\n    v UInt64 TTL d + INTERVAL 1 DAY"
        engine = "MergeTree"
        order_by = "k"
        engine_settings = ""
    elif row_ttl:
        if row_reducing or summing_merge or scenario != "normal":
            raise ValueError("row-TTL workload supports only the ordinary normal projection")
        columns = "k UInt64,\n    d Date,\n    v UInt64"
        engine = "MergeTree"
        order_by = "k"
        table_ttl = "TTL d + INTERVAL 1 DAY"
        engine_settings = ""
    elif summing_merge:
        if row_reducing or scenario != "normal":
            raise ValueError("summing workload supports only the ordinary normal projection")
        columns = "k UInt64,\n    v UInt64"
        engine = "SummingMergeTree"
        order_by = "k"
        engine_settings = ",\n    deduplicate_merge_projection_mode = 'rebuild'"
    elif row_reducing:
        columns = "k UInt64,\n    version UInt64,\n    v UInt64"
        engine = "ReplacingMergeTree(version)"
        order_by = "k"
        engine_settings = ",\n    deduplicate_merge_projection_mode = 'rebuild'"
    else:
        columns = "k UInt64,\n    v UInt64"
        if scenario == "dynamic":
            columns += ",\n    raw String"
        engine = "MergeTree"
        order_by = "(k, v)"
        engine_settings = ""

    extra_columns = vertical_extra_columns(vertical_mode)
    if vertical_mode == "forced":
        vertical_settings = """    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
"""
    elif vertical_mode == "default":
        vertical_settings = ""
    elif vertical_mode == "disabled":
        vertical_settings = "    enable_vertical_merge_algorithm = 0,\n"
    else:
        raise ValueError(f"unknown vertical mode: {vertical_mode}")

    if extra_columns:
        columns += "".join(f",\n    {column} UInt64" for column in extra_columns)

    use_default_part_format = wide_part_rows is None
    if use_default_part_format:
        part_format_settings = ""
    else:
        part_format_settings = f""",
    min_rows_for_wide_part = {wide_part_rows},
    min_bytes_for_wide_part = 0,
    min_level_for_wide_part = 0"""

    source_part_count = SOURCE_PARTS * 2 if expired_source_column else SOURCE_PARTS
    source_rows_per_part = rows_per_part // 2 if expired_source_column else rows_per_part
    if expired_source_column and source_rows_per_part * 2 != rows_per_part:
        raise ValueError("expired-source rows_per_part must be even")

    merge_block_setting = "" if merge_max_block_size is None else f",\n    merge_max_block_size = {merge_max_block_size}"
    index_setting = ",\n    index_granularity = 1" if scenario == "index" else ""
    dynamic_setting = (
        ",\n    merge_max_dynamic_subcolumns_in_wide_part = 4,\n    merge_max_dynamic_subcolumns_in_compact_part = 10"
        if scenario == "dynamic"
        else ""
    )
    commit_order_setting = (
        ",\n    enable_block_number_column = 1,\n    enable_block_offset_column = 1,\n    allow_commit_order_projection = 1"
        if scenario == "commit_order"
        else ""
    )
    optimize_row_order_setting = ",\n    optimize_row_order = 1" if optimize_row_order else ""
    selector_setting = "" if use_default_selector else ",\n    merge_selector_algorithm = 'Manual'"
    query(binary, port, "CREATE DATABASE bench")
    # Horizontal modes explicitly disable vertical merges. The forced mode lowers
    # activation thresholds, while the default mode leaves all vertical settings unset.
    query(
        binary,
        port,
        f"""
CREATE TABLE bench.t
(
    {columns},
    {projection}
)
ENGINE = {engine}
ORDER BY {order_by}
{table_ttl}
SETTINGS
{vertical_settings}    materialize_projections_on_insert = {int(expired_source_column)},
    materialize_projections_on_merge = {int(not expired_source_column)}{engine_settings}{merge_block_setting}{index_setting}{dynamic_setting}{commit_order_setting}{optimize_row_order_setting}{selector_setting}{part_format_settings}
""",
    )
    if row_ttl:
        query(binary, port, "SYSTEM STOP TTL MERGES bench.t")
    if use_default_selector:
        query(binary, port, "SYSTEM STOP MERGES bench.t")

    if use_default_part_format:
        create_statement = query(binary, port, "SHOW CREATE TABLE bench.t")
        format_settings = ("min_rows_for_wide_part", "min_bytes_for_wide_part", "min_level_for_wide_part")
        if any(setting in create_statement for setting in format_settings):
            raise RuntimeError(f"default part-format workload set a format threshold: {create_statement!r}")

    for part in range(source_part_count):
        offset = part * source_rows_per_part
        if expired_source_column:
            select = f"number % {projection_groups}, toDate('2000-01-01'), cityHash64(number + {offset})"
        elif row_ttl:
            select = (
                f"number % {projection_groups}, if(number % 2 = 0, toDate('2000-01-01'), toDate('2100-01-01')), "
                f"cityHash64(number + {offset})"
            )
        elif row_reducing:
            select = f"number, toUInt64({part}), number + {offset}"
        elif scenario == "dynamic":
            select = (
                f"number % {projection_groups}, cityHash64(number + {offset}), "
                f"concat('{{\"a\":', toString(number + {offset}), ',\"b\":', toString(number + {offset}), "
                f"',\"c\":', toString(number + {offset}), ',\"d\":', toString(number + {offset}), "
                f"',\"e\":', toString(number + {offset}), ',\"f\":', toString(number + {offset}), '}}')"
            )
        else:
            select = f"number % {projection_groups}, cityHash64(number + {offset})"
        if extra_columns:
            select += "".join(f", number + {offset}" for _ in extra_columns)
        query(
            binary,
            port,
            f"""INSERT INTO bench.t
SELECT {select}
FROM numbers({source_rows_per_part})
SETTINGS max_block_size = {source_rows_per_part}, min_insert_block_size_rows = {source_rows_per_part}, min_insert_block_size_bytes = 0""",
        )


def run_merge(
    binary,
    source_root,
    output_root,
    name,
    projection_groups,
    rows_per_part,
    scenario,
    row_reducing,
    vertical_mode,
    wide_part_rows,
    expired_source_column,
    summing_merge,
    row_ttl,
    expected_projection_part_type,
    background_squash_rows,
    merge_max_block_size,
    use_default_selector,
    optimize_row_order,
):
    work = output_root / name
    shutil.rmtree(work, ignore_errors=True)
    for directory in ("data", "user_files", "log"):
        (work / directory).mkdir(parents=True, exist_ok=True)
    server_config = prepare_server_config(source_root, work, background_squash_rows)

    tcp_port, http_port, interserver_port = free_port(), free_port(), free_port()
    server_log = (work / "log/server.log").open("w", encoding="utf-8")
    server = subprocess.Popen(
        [
            str(binary),
            "server",
            "--config-file",
            str(server_config),
            "--pid-file",
            str(work / "server.pid"),
            "--",
            "--path",
            str(work / "data"),
            "--user_files_path",
            str(work / "user_files"),
            "--tcp_port",
            str(tcp_port),
            "--http_port",
            str(http_port),
            "--interserver_http_port",
            str(interserver_port),
            "--logger.log",
            str(work / "log/clickhouse.log"),
            "--logger.errorlog",
            str(work / "log/error.log"),
            "--logger.stderr",
            str(work / "log/stderr.log"),
        ],
        stdout=server_log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        cwd=work,
    )
    pid = None
    sampler = None
    stop_sampling = threading.Event()
    try:
        for _ in range(400):
            try:
                query(binary, tcp_port, "SELECT 1")
                pid = int((work / "server.pid").read_text(encoding="ascii").strip())
                break
            except (FileNotFoundError, RuntimeError, ValueError):
                if server.poll() is not None:
                    raise RuntimeError(f"server exited during startup; see {work / 'log/server.log'}")
                time.sleep(0.05)
        else:
            raise RuntimeError(f"server did not start; see {work / 'log/server.log'}")

        create_table(
            binary,
            tcp_port,
            projection_groups,
            rows_per_part,
            scenario,
            row_reducing,
            vertical_mode,
            wide_part_rows,
            expired_source_column,
            summing_merge,
            row_ttl,
            merge_max_block_size,
            use_default_selector,
            optimize_row_order,
        )

        parts = prepare_expired_source_parts(binary, tcp_port) if expired_source_column else active_part_names(binary, tcp_port)
        if len(parts) != SOURCE_PARTS:
            raise RuntimeError(f"expected {SOURCE_PARTS} active source parts, got {parts!r}")
        source_projection_parts = query(
            binary,
            tcp_port,
            "SELECT count() FROM system.projection_parts WHERE database = 'bench' AND table = 't' AND active",
        ).strip()
        expected_source_projection_parts = str(SOURCE_PARTS if expired_source_column else 0)
        if source_projection_parts != expected_source_projection_parts:
            raise RuntimeError(f"unexpected source projection parts: {source_projection_parts!r}")

        before = profile_events(binary, tcp_port)
        peak_rss_kib = [process_rss_kib(pid)]

        def sample_rss():
            while not stop_sampling.wait(0.005):
                try:
                    peak_rss_kib[0] = max(peak_rss_kib[0], process_rss_kib(pid))
                except FileNotFoundError:
                    return

        sampler = threading.Thread(target=sample_rss, daemon=True)
        sampler.start()
        started = time.monotonic()
        # Keep TTL processing disabled while the source parts are inserted, then
        # enable it immediately before the explicitly measured merge. Otherwise
        # the test either loses its fixed source-part boundary or never exercises
        # row expiration at all.
        if row_ttl:
            query(binary, tcp_port, "SYSTEM START TTL MERGES bench.t")
        if use_default_selector:
            sync_default_selector_merges(binary, tcp_port)
        else:
            sync_parts_merge(binary, tcp_port, parts)
        elapsed = time.monotonic() - started
        stop_sampling.set()
        sampler.join(timeout=1)

        after = profile_events(binary, tcp_port)
        delta = {event: after[event] - before[event] for event in EVENTS}
        # These task-resource counters are finalized by registered `ClickHouse` task
        # scopes. Sample them after the merge and before validation instead of using
        # process-wide /proc CPU accounting, which includes unrelated daemon work.
        cpu_seconds = (delta["UserTimeMicroseconds"] + delta["SystemTimeMicroseconds"]) / 1_000_000
        if peak_rss_kib[0] <= 0 or cpu_seconds < 0:
            raise RuntimeError(f"invalid merge-resource samples: cpu={cpu_seconds}, rss={peak_rss_kib!r}")

        validate_merge(
            binary,
            tcp_port,
            projection_groups,
            rows_per_part,
            scenario,
            row_reducing,
            summing_merge,
            row_ttl,
            vertical_mode,
            expired_source_column,
            expected_projection_part_type,
        )

        if vertical_mode != "disabled" and delta["MergeVerticalStageExecuteMilliseconds"] <= 0:
            raise RuntimeError(f"merge did not execute the vertical stage: {delta!r}")
        expected_rebuilt_projections = len(MULTI_PROJECTION_SCENARIOS) if scenario == "multi" else 1
        if (
            (use_default_selector and (delta["RebuiltProjections"] <= 0 or delta["MergedProjections"] != 0))
            or (
                not use_default_selector
                and (delta["RebuiltProjections"] != expected_rebuilt_projections or delta["MergedProjections"] != 0)
            )
        ):
            raise RuntimeError(f"merge did not rebuild the expected projection set: {delta!r}")
        if expired_source_column:
            parent_v_columns = query(
                binary,
                tcp_port,
                "SELECT count() FROM system.parts_columns WHERE database = 'bench' AND table = 't' AND active AND column = 'v'",
            ).strip()
            if parent_v_columns != "0":
                raise RuntimeError(f"final expired-column parent unexpectedly contains v: {parent_v_columns!r}")

        final_parts = delta["MergeTreeDataProjectionWriterFinalParts"]
        temporary_parts = delta["MergeTreeDataProjectionWriterBlocks"]
        # Both arms account projection serialization with the corresponding
        # versioned event: the archived event covered temporary rebuilt parts
        # and recursive outputs, while the renamed event also covers final
        # rebuild output and `Native` runs.
        projection_serialization_bytes = delta[PROJECTION_SERIALIZATION_EVENT]
        if delta["MergeMutateBackgroundExecutorTaskExecuteStepMicroseconds"] <= 0:
            raise RuntimeError(f"merge did not execute through the background executor: {delta!r}")
        if delta["ReadBufferFromFileDescriptorReadBytes"] <= 0:
            raise RuntimeError(f"merge did not read compressed part data: {delta!r}")
        if projection_serialization_bytes <= 0:
            raise RuntimeError(f"merge did not write projection data: {delta!r}")
        # The commit event is intentionally recursive-only. Each legacy
        # temporary projection part is committed once, so the historical total
        # is its initial temporary-part count plus recursive commits. The
        # streamed arm has neither kind of temporary commit.
        if final_parts:
            expected_final_parts = len(MULTI_PROJECTION_SCENARIOS) if scenario == "multi" else 1
            if (not use_default_selector and final_parts != expected_final_parts) or temporary_parts:
                raise RuntimeError(f"streamed writer did not produce the expected final parts: {delta!r}")
            temporary_part_commits = delta[RECURSIVE_PROJECTION_PART_MERGE_COMMITS_EVENT]
            if temporary_part_commits:
                raise RuntimeError(f"streamed rebuild committed a recursive projection-part merge: {delta!r}")
            reordered_projection = (
                scenario in {"reordered", "reordered_aggregate", "parent_offset", "filtered_reordered", "index", "commit_order", "multi"}
                or optimize_row_order
            )
            requires_external_spill = (
                scenario in {"reordered", "reordered_aggregate", "parent_offset", "commit_order", "multi"} or optimize_row_order
            )
            if not reordered_projection and delta["ExternalSortCompressedBytes"]:
                raise RuntimeError(f"order-compatible projection unexpectedly spilled: {delta!r}")
            if requires_external_spill and delta["ExternalSortCompressedBytes"] <= 0:
                raise RuntimeError(f"reordered projection did not spill an external run: {delta!r}")
            if scenario == "reordered" and not row_reducing and delta["ExternalSortWritePart"] <= LEGACY_PROJECTION_MERGE_FAN_IN:
                raise RuntimeError(
                    "reordered streamed projection did not spill more than one legacy fan-in "
                    f"of bounded runs: {delta!r}"
                )
        else:
            if temporary_parts <= 0:
                raise RuntimeError(f"legacy merge did not write temporary projection parts: {delta!r}")
            temporary_part_commits = temporary_parts + delta[RECURSIVE_PROJECTION_PART_MERGE_COMMITS_EVENT]
            if temporary_part_commits <= 0:
                raise RuntimeError(f"legacy merge did not record projection-part commits: {delta!r}")

        return {
            "merge_seconds": elapsed,
            "cpu_seconds": cpu_seconds,
            "executor_step_seconds": delta["MergeMutateBackgroundExecutorTaskExecuteStepMicroseconds"] / 1_000_000,
            "merge_file_read_compressed_bytes": delta["ReadBufferFromFileDescriptorReadBytes"],
            "merge_file_write_compressed_bytes": delta["WriteBufferFromFileDescriptorWriteBytes"],
            "projection_serialization_compressed_bytes": projection_serialization_bytes,
            "external_sort_compressed_bytes": delta["ExternalSortCompressedBytes"],
            "temporary_projection_part_writes": temporary_parts,
            "temporary_projection_part_commits": temporary_part_commits,
            "peak_rss_kib": peak_rss_kib[0],
        }
    finally:
        stop_sampling.set()
        if sampler is not None:
            sampler.join(timeout=1)
        for process_id in {pid, server.pid}:
            if process_id is not None:
                try:
                    os.kill(process_id, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(server.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            server.wait(timeout=10)
        server_log.close()


def main():
    args = parse_args()
    binary = args.clickhouse.resolve()
    source_root = args.source_root.resolve()
    output_root = args.output_root.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(f"clickhouse executable is not executable: {binary}")
    if not (source_root / "programs/server/config.xml").is_file():
        raise RuntimeError(f"source root does not contain server configuration: {source_root}")

    workload = WORKLOADS[args.mode]
    output_root.mkdir(parents=True, exist_ok=True)
    results = {}
    for scenario in workload["scenarios"]:
        # These ordinary aggregate merges are short enough that task CPU counters
        # have visible granularity. The reducing aggregate and unordered cases also
        # use a fixed batch: they exercise bounded output state or external runs, and
        # report the batch's maximum RSS instead of treating a single allocator sample
        # as representative. This is a fixed workload, not a retry of a failed merge.
        if args.mode == "ordinary" and scenario in AGGREGATE_SCENARIOS:
            repetitions = ORDINARY_AGGREGATE_REPETITIONS
        elif args.mode == "reducing" and scenario in REDUCING_BATCH_SCENARIOS:
            repetitions = REDUCING_BATCH_REPETITIONS
        else:
            repetitions = 1
        samples = [
            run_merge(
                binary,
                source_root,
                output_root,
                f"{args.mode}-{scenario}-{repetition}",
                workload["projection_groups"],
                workload["rows_per_part"],
                scenario=scenario,
                row_reducing=workload["row_reducing"],
                vertical_mode=workload["vertical_mode"],
                wide_part_rows=workload["wide_part_rows"],
                expired_source_column=workload.get("expired_source_column", False),
                summing_merge=workload.get("summing_merge", False),
                row_ttl=workload.get("row_ttl", False),
                expected_projection_part_type=(
                    workload["expected_projection_part_types"]
                    if scenario == "multi"
                    else workload["expected_projection_part_types"][scenario]
                ),
                background_squash_rows=workload.get("background_squash_rows", BACKGROUND_SQUASH_ROWS),
                merge_max_block_size=workload.get("merge_max_block_size", BACKGROUND_SQUASH_ROWS),
                use_default_selector=workload.get("use_default_selector", False),
                optimize_row_order=workload.get("optimize_row_order", False),
            )
            for repetition in range(repetitions)
        ]
        results[scenario] = {
            suffix: (
                max(sample[suffix] for sample in samples)
                if suffix == "peak_rss_kib"
                else sum(sample[suffix] for sample in samples)
            )
            for suffix in METRIC_SUFFIXES
        }
    metrics = {}
    if args.aggregate_metrics:
        for suffix in METRIC_SUFFIXES:
            metrics[f"{args.mode}_{suffix}"] = (
                max(values[suffix] for values in results.values())
                if suffix == "peak_rss_kib"
                else sum(values[suffix] for values in results.values())
            )
    else:
        for scenario, values in results.items():
            for suffix in METRIC_SUFFIXES:
                metrics[f"{args.mode}_{scenario}_{suffix}"] = values[suffix]
    for name, value in metrics.items():
        if not isinstance(value, (int, float)) or value < 0:
            raise RuntimeError(f"invalid {name}: {value!r}")
        print(json.dumps({"metric": name, "value": value}, separators=(",", ":")))


if __name__ == "__main__":
    main()

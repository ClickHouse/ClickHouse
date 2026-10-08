#!/usr/bin/env python3
"""Persistent `MergeTree` benchmark for `radix_join` and `parallel_hash`.

The loader creates deterministic build and probe tables in a `clickhouse local`
data path. The runner validates those tables, checks result correctness, and
then measures wall latency from `clickhouse local --time` and counters from
final query-scoped ProfileEvents packets. Only the Python standard library is
used.
"""

from __future__ import annotations

import argparse
import dataclasses
import decimal
import hashlib
import json
import os
import pathlib
import re
import statistics
import subprocess
import sys
from collections.abc import Sequence


DEFAULT_BINARY = "/mnt/ch/ClickHouse/build/reldeb/programs/clickhouse"
DEFAULT_PATH = "tmp/join_bench_data"
DEFAULT_MAX_MEMORY = 100_000_000_000
DEFAULT_VERIFY_MAX_OUTPUT_ROWS = 10_000_000
SCHEMA_VERSION = 4
GENERATOR_SIGNATURE = "join-mergetree-generator-v4"

BUILD_TABLE = "join_mergetree_bench_build"
PROBE_TABLE = "join_mergetree_bench_probe"
METADATA_TABLE = "join_mergetree_bench_metadata"

KEY_SEED = 11_400_714_819_323_198_485
SHUFFLE_SEED = 14_029_467_366_897_019_727
PROBE_CYCLE_SEED = 1_609_587_929_392_839_161
AFFINE_SEED = 9_650_029_242_287_828_579
OCCURRENCE_MIX = 2_874_177_450_012_600_261
BUCKET_MIX = 1
BUILD_PAYLOAD_SEED = 13_777_605_477_067_454_941
PROBE_PAYLOAD_SEED = 4_354_685_564_936_845_355
PAYLOAD_COLUMN_MIX = 7_046_029_254_386_353_131
MISS_DOMAIN_BIT = 1 << 63
UINT64_MAX = (1 << 64) - 1

ASSERT_MARKER = "__JOIN_MERGETREE_ASSERT__"

EVENTS = (
    "RealTimeMicroseconds",
    "SelectedRows",
    "SelectedBytes",
    "RadixHashJoinBuildMicroseconds",
    "RadixHashJoinProbeMicroseconds",
    "RadixHashJoinProbeCollectMatchesMicroseconds",
    "RadixHashJoinProbePackHashRouteMicroseconds",
    "RadixHashJoinLeafGroupBuilds",
    "RadixHashJoinLeafGroupBuildMicroseconds",
    "HashJoinProbeMatchMicroseconds",
    "HashJoinProbeGatherMicroseconds",
    "ConcurrentHashJoinProbeDispatchMicroseconds",
    "MemoryTrackerPeakUsage",
)
# `MemoryTrackerPeakUsage` is a `(gauge)` snapshot of the query memory tracker's
# peak, not a `(increment)` counter like the rest of `EVENTS`.
GAUGE_EVENTS = frozenset({"MemoryTrackerPeakUsage"})
WALL_TIME_EVENT = "WallTimeMicroseconds"

ALGORITHMS = ("radix_join", "parallel_hash")

BUILD_FIXED_COLUMNS = (
    ("occurrence", "UInt64"),
    ("card_bucket", "UInt64"),
    ("selector", "UInt64"),
    ("k", "UInt64"),
    ("shuffle_rank", "UInt64"),
)
PROBE_FIXED_COLUMNS = (
    ("cycle", "UInt64"),
    ("card_bucket", "UInt64"),
    ("rank", "UInt64"),
    ("hit_k", "UInt64"),
    ("miss_k", "UInt64"),
)
METADATA_COLUMNS = (
    ("schema_version", "UInt64"),
    ("generator_signature", "String"),
    ("max_cardinality", "UInt64"),
    ("bucket_width", "UInt64"),
    ("max_multiplicity", "UInt64"),
    ("max_cycles", "UInt64"),
    ("max_build_payload_columns", "UInt64"),
    ("max_probe_payload_columns", "UInt64"),
    ("build_part_fingerprint", "String"),
    ("probe_part_fingerprint", "String"),
)


@dataclasses.dataclass(frozen=True)
class LoadedMetadata:
    schema_version: int
    max_cardinality: int
    bucket_width: int
    max_multiplicity: int
    max_cycles: int
    max_build_payload_columns: int = 1
    max_probe_payload_columns: int = 1
    generator_signature: str = GENERATOR_SIGNATURE
    build_part_fingerprint: str = ""
    probe_part_fingerprint: str = ""

    @property
    def cardinalities(self) -> tuple[int, ...]:
        return tuple(
            range(self.bucket_width, self.max_cardinality + 1, self.bucket_width)
        )


@dataclasses.dataclass(frozen=True)
class BenchmarkPoint:
    cardinality: int
    multiplicity: int
    ratio: decimal.Decimal
    hit_rate: decimal.Decimal
    probe_rows: int
    hit_rows: int
    bucket_width: int = 1
    build_payload_columns: int = 1
    probe_payload_columns: int = 1

    @property
    def build_rows(self) -> int:
        return self.cardinality * self.multiplicity

    @property
    def output_rows(self) -> int:
        return self.hit_rows * self.multiplicity

    @property
    def label(self) -> str:
        return (
            f"D={self.cardinality} m={self.multiplicity} "
            f"ratio={canonical_decimal(self.ratio)} "
            f"hit={canonical_decimal(self.hit_rate)} "
            f"bp={self.build_payload_columns} pp={self.probe_payload_columns}"
        )


@dataclasses.dataclass(frozen=True)
class Measurements:
    median_us: int | float
    min_us: int
    events: dict[str, int]


@dataclasses.dataclass
class AlgorithmResult:
    algorithm: str
    status: str
    measurements: Measurements | None = None
    detail: str = ""


def canonical_decimal(value: decimal.Decimal) -> str:
    """Return a non-exponent canonical spelling for a finite decimal."""
    if not value.is_finite():
        raise ValueError("decimal value must be finite")
    result = format(value, "f")
    if "." in result:
        result = result.rstrip("0").rstrip(".")
    if result in ("", "-0"):
        return "0"
    return result


def mix_seed_sql(base: int, cycle_or_occurrence: str, card_bucket: str) -> str:
    return (
        f"bitXor(bitXor(toUInt64({base}), "
        f"{cycle_or_occurrence} * toUInt64({OCCURRENCE_MIX})), "
        f"{card_bucket} * toUInt64({BUCKET_MIX}))"
    )


def _tokens(text: str, name: str) -> list[str]:
    if not text:
        raise ValueError(f"{name} must not be empty")
    values = [item.strip() for item in text.split(",")]
    if any(not item for item in values):
        raise ValueError(f"{name} must be an exact comma-separated list")
    return values


def parse_integer_list(text: str, name: str) -> list[int]:
    values: list[int] = []
    for token in _tokens(text, name):
        if not token.isdecimal():
            raise ValueError(f"{name} contains a non-integer value: {token!r}")
        value = int(token)
        if value <= 0:
            raise ValueError(f"{name} values must be positive")
        values.append(value)
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must not contain duplicates")
    return values


def parse_nonnegative_integer_list(text: str, name: str) -> list[int]:
    values: list[int] = []
    for token in _tokens(text, name):
        if token.startswith("-") and token[1:].isdecimal():
            raise ValueError(f"{name} values must be nonnegative")
        if not token.isdecimal():
            raise ValueError(f"{name} contains a non-integer value: {token!r}")
        values.append(int(token))
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must not contain duplicates")
    return values


def parse_decimal_list(text: str, name: str, *, allow_zero: bool = False) -> list[decimal.Decimal]:
    values: list[decimal.Decimal] = []
    for token in _tokens(text, name):
        try:
            value = decimal.Decimal(token)
        except decimal.InvalidOperation as ex:
            raise ValueError(f"{name} contains an invalid decimal: {token!r}") from ex
        if not value.is_finite():
            raise ValueError(f"{name} values must be finite")
        if value < 0 or (value == 0 and not allow_zero):
            qualifier = "nonnegative" if allow_zero else "positive"
            raise ValueError(f"{name} values must be {qualifier}")
        values.append(value)
    canonical = [canonical_decimal(value) for value in values]
    if len(canonical) != len(set(canonical)):
        raise ValueError(f"{name} must not contain duplicates")
    return values


def round_hit_count(probe_rows: int, hit_rate: decimal.Decimal) -> int:
    """Round `N_p * h` to nearest integer, with exact decimal halves rounded up."""
    if probe_rows < 0 or not hit_rate.is_finite() or hit_rate < 0:
        raise ValueError("probe rows and hit rate must be finite and nonnegative")
    sign, digits, exponent = hit_rate.as_tuple()
    exponent = int(exponent)
    coefficient = int("".join(str(digit) for digit in digits) or "0")
    if sign:
        coefficient = -coefficient
    numerator = probe_rows * coefficient
    denominator = 1
    if exponent >= 0:
        numerator *= 10 ** exponent
    else:
        denominator = 10 ** (-exponent)
    quotient, remainder = divmod(numerator, denominator)
    return quotient + int(remainder * 2 >= denominator)


def validate_probe_domain(probe_rows: int) -> None:
    """Ensure a selected probe count remains in the generated row domain."""
    if probe_rows <= 0:
        raise ValueError("probe selection must contain at least one row")
    if probe_rows > MISS_DOMAIN_BIT:
        raise ValueError("probe selection exceeds the disjoint UInt64 miss-key domain")


def _exact_decimal_integer_product(base: int, factor: decimal.Decimal) -> int | None:
    sign, digits, exponent = factor.as_tuple()
    exponent = int(exponent)
    coefficient = int("".join(str(digit) for digit in digits) or "0")
    if sign:
        coefficient = -coefficient
    numerator = base * coefficient
    if exponent >= 0:
        return numerator * (10 ** exponent)
    quotient, remainder = divmod(numerator, 10 ** (-exponent))
    return quotient if remainder == 0 else None


def validate_load_dimensions(metadata: LoadedMetadata) -> None:
    if metadata.max_cardinality <= 0 or metadata.max_cardinality >= MISS_DOMAIN_BIT:
        raise ValueError(
            f"max cardinality must be positive and less than {MISS_DOMAIN_BIT}"
        )
    if metadata.bucket_width <= 0 or metadata.bucket_width & (metadata.bucket_width - 1):
        raise ValueError("bucket width must be a positive power of two")
    if metadata.max_cardinality % metadata.bucket_width:
        raise ValueError("bucket width must divide max cardinality")
    if not 1 <= metadata.max_multiplicity <= 64:
        raise ValueError("max multiplicity must be in [1, 64]")
    if not 1 <= metadata.max_cycles <= 128:
        raise ValueError("max cycles must be in [1, 128]")
    if metadata.max_cardinality * metadata.max_cycles > MISS_DOMAIN_BIT:
        raise ValueError(
            "max cardinality multiplied by max cycles exceeds the disjoint "
            "miss-key domain"
        )
    if (
        metadata.max_build_payload_columns < 0
        or metadata.max_probe_payload_columns < 0
    ):
        raise ValueError("maximum payload column counts must be nonnegative")


def estimate_raw_bytes(metadata: LoadedMetadata) -> int:
    build = (
        metadata.max_cardinality
        * metadata.max_multiplicity
        * 8
        * (5 + metadata.max_build_payload_columns)
    )
    probe = (
        metadata.max_cardinality
        * metadata.max_cycles
        * 8
        * (5 + metadata.max_probe_payload_columns)
    )
    return build + probe


def validate_load_capacity(metadata: LoadedMetadata, *, free_bytes: int) -> int:
    if free_bytes < 0:
        raise ValueError("free space must be nonnegative")
    raw_bytes = estimate_raw_bytes(metadata)
    if raw_bytes * 10 > free_bytes * 9:
        raise ValueError(
            f"estimated raw data size {raw_bytes} bytes exceeds 90% of free "
            f"space ({free_bytes} bytes)"
        )
    return raw_bytes


def _filesystem_free_bytes(path: str) -> int:
    stats = os.statvfs(path)
    return stats.f_bavail * stats.f_frsize


def validate_points(
    metadata: LoadedMetadata,
    cardinalities: Sequence[int],
    multiplicities: Sequence[int],
    ratios: Sequence[decimal.Decimal],
    hit_rates: Sequence[decimal.Decimal],
    build_payload_columns: Sequence[int] = (1,),
    probe_payload_columns: Sequence[int] = (1,),
) -> list[BenchmarkPoint]:
    validate_load_dimensions(metadata)
    points: list[BenchmarkPoint] = []
    for cardinality in cardinalities:
        if (
            cardinality < metadata.bucket_width
            or cardinality > metadata.max_cardinality
            or cardinality % metadata.bucket_width
        ):
            raise ValueError(
                f"cardinality {cardinality} must be a multiple of bucket width "
                f"{metadata.bucket_width} in [{metadata.bucket_width}, "
                f"max cardinality {metadata.max_cardinality}]"
            )
    for multiplicity in multiplicities:
        if multiplicity < 1 or multiplicity > metadata.max_multiplicity:
            raise ValueError(
                f"multiplicity {multiplicity} is outside 1..{metadata.max_multiplicity}"
            )
    for ratio in ratios:
        if not ratio.is_finite() or ratio <= 0:
            raise ValueError(
                f"ratio {canonical_decimal(ratio)} must be finite and positive"
            )
    for hit_rate in hit_rates:
        if not hit_rate.is_finite() or hit_rate < 0 or hit_rate > 1:
            raise ValueError(f"hit rate {hit_rate} is outside [0, 1]")
    for count in build_payload_columns:
        if count < 0 or count > metadata.max_build_payload_columns:
            raise ValueError(
                f"build payload column count {count} is outside [0, loaded maximum "
                f"{metadata.max_build_payload_columns}]"
            )
    for count in probe_payload_columns:
        if count < 0 or count > metadata.max_probe_payload_columns:
            raise ValueError(
                f"probe payload column count {count} is outside [0, loaded maximum "
                f"{metadata.max_probe_payload_columns}]"
            )

    for cardinality in cardinalities:
        for multiplicity in multiplicities:
            for ratio in ratios:
                if ratio * multiplicity > metadata.max_cycles:
                    raise ValueError(
                        f"ratio {canonical_decimal(ratio)} * multiplicity "
                        f"{multiplicity} exceeds max cycles {metadata.max_cycles}"
                    )
                probe_rows = _exact_decimal_integer_product(
                    cardinality * multiplicity, ratio
                )
                if probe_rows is None:
                    raise ValueError(
                        f"D={cardinality}, m={multiplicity}, "
                        f"ratio={canonical_decimal(ratio)} does not produce an exact integer N_p"
                    )
                if probe_rows > metadata.max_cycles * cardinality:
                    raise ValueError(
                        f"N_p={probe_rows} exceeds max cycles {metadata.max_cycles} "
                        f"times D={cardinality}"
                    )
                validate_probe_domain(probe_rows)
                for hit_rate in hit_rates:
                    hit_rows = round_hit_count(probe_rows, hit_rate)
                    if hit_rows > UINT64_MAX // multiplicity:
                        raise ValueError(
                            "joined output row count exceeds the UInt64 domain"
                        )
                    for build_count in build_payload_columns:
                        for probe_count in probe_payload_columns:
                            points.append(
                                BenchmarkPoint(
                                    cardinality=cardinality,
                                    multiplicity=multiplicity,
                                    ratio=ratio,
                                    hit_rate=hit_rate,
                                    probe_rows=probe_rows,
                                    hit_rows=hit_rows,
                                    bucket_width=metadata.bucket_width,
                                    build_payload_columns=build_count,
                                    probe_payload_columns=probe_count,
                                )
                            )
    return points


def _payload_columns(prefix: str, count: int) -> tuple[tuple[str, str], ...]:
    if count < 0:
        raise ValueError("payload column count must be nonnegative")
    return tuple((f"{prefix}{index}", "UInt64") for index in range(count))


def build_columns(count: int) -> tuple[tuple[str, str], ...]:
    return BUILD_FIXED_COLUMNS + _payload_columns("b_p", count)


def probe_columns(count: int) -> tuple[tuple[str, str], ...]:
    return PROBE_FIXED_COLUMNS + _payload_columns("p_p", count)


def _column_definitions(columns: Sequence[tuple[str, str]]) -> str:
    return ",\n    ".join(f"{name} {type_name}" for name, type_name in columns)


def recreate_schema_sql(
    max_build_payload_columns: int = 1, max_probe_payload_columns: int = 1
) -> str:
    build_definitions = _column_definitions(
        build_columns(max_build_payload_columns)
    )
    probe_definitions = _column_definitions(
        probe_columns(max_probe_payload_columns)
    )
    metadata_definitions = _column_definitions(METADATA_COLUMNS)
    return f"""
DROP TABLE IF EXISTS {METADATA_TABLE};
DROP TABLE IF EXISTS {PROBE_TABLE};
DROP TABLE IF EXISTS {BUILD_TABLE};

CREATE TABLE {BUILD_TABLE}
(
    {build_definitions}
)
ENGINE = MergeTree
PARTITION BY occurrence
ORDER BY (occurrence, card_bucket, shuffle_rank);

CREATE TABLE {PROBE_TABLE}
(
    {probe_definitions}
)
ENGINE = MergeTree
PARTITION BY cycle
ORDER BY (cycle, card_bucket, rank);

CREATE TABLE {METADATA_TABLE}
(
    {metadata_definitions}
)
ENGINE = MergeTree
ORDER BY tuple();
""".strip()


def _payload_seed(base: int, index: int) -> int:
    return (base + index * PAYLOAD_COLUMN_MIX) & UINT64_MAX


def build_insert_sql(metadata: LoadedMetadata) -> str:
    rows = metadata.max_cardinality * metadata.max_multiplicity
    payload_selects = [
        (
            "intHash64(bitXor(bitXor(selector, occurrence * "
            f"toUInt64({OCCURRENCE_MIX})), "
            f"toUInt64({_payload_seed(BUILD_PAYLOAD_SEED, index)}))) AS b_p{index}"
        )
        for index in range(metadata.max_build_payload_columns)
    ]
    select_suffix = "".join(f",\n    {expression}" for expression in payload_selects)
    return f"""
INSERT INTO {BUILD_TABLE}
SELECT
    occurrence,
    card_bucket,
    selector,
    intHash64(bitXor(selector, toUInt64({KEY_SEED}))) AS k,
    intHash64(
        bitXor(
            selector,
            intHash64(
                {mix_seed_sql(SHUFFLE_SEED, "occurrence", "card_bucket")}
            )
        )
    ) AS shuffle_rank{select_suffix}
FROM
(
    SELECT
        number,
        toUInt64(number % {metadata.max_cardinality}) AS selector,
        toUInt64(intDiv(number, {metadata.max_cardinality})) AS occurrence,
        toUInt64(
            intDiv(number % {metadata.max_cardinality}, {metadata.bucket_width})
        ) AS card_bucket
    FROM numbers({rows})
)
SETTINGS max_partitions_per_insert_block = {
    max(metadata.max_multiplicity, metadata.max_cycles)
}
""".strip()


def probe_insert_sql(metadata: LoadedMetadata) -> str:
    rows = metadata.max_cardinality * metadata.max_cycles
    payload_selects = [
        (
            f"intHash64(bitXor(global_row, "
            f"toUInt64({_payload_seed(PROBE_PAYLOAD_SEED, index)}))) AS p_p{index}"
        )
        for index in range(metadata.max_probe_payload_columns)
    ]
    select_suffix = "".join(f",\n    {expression}" for expression in payload_selects)
    return f"""
INSERT INTO {PROBE_TABLE}
SELECT
    cycle,
    card_bucket,
    rank,
    intHash64(bitXor(hit_selector, toUInt64({KEY_SEED}))) AS hit_k,
    intHash64(
        bitXor(
            bitOr(toUInt64({MISS_DOMAIN_BIT}), global_row),
            toUInt64({KEY_SEED})
        )
    ) AS miss_k{select_suffix}
FROM
(
    SELECT
        global_row,
        cycle,
        card_bucket,
        rank,
        toUInt64(
            toUInt128(card_bucket) * {metadata.bucket_width}
            + toUInt64(
                (
                    toUInt128(rank) * toUInt128(
                        bitOr(
                            intHash64(
                                {mix_seed_sql(AFFINE_SEED, "cycle", "card_bucket")}
                            ),
                            toUInt64(1)
                        )
                    )
                    + toUInt128(
                        intHash64(
                            {mix_seed_sql(PROBE_CYCLE_SEED, "cycle", "card_bucket")}
                        ) % {metadata.bucket_width}
                    )
                ) % toUInt128({metadata.bucket_width})
            )
        ) AS hit_selector
    FROM
    (
        SELECT
            number AS global_row,
            toUInt64(intDiv(number, {metadata.max_cardinality})) AS cycle,
            toUInt64(
                intDiv(number % {metadata.max_cardinality}, {metadata.bucket_width})
            ) AS card_bucket,
            toUInt64(number % {metadata.bucket_width}) AS rank
        FROM numbers({rows})
    )
)
SETTINGS max_partitions_per_insert_block = {
    max(metadata.max_multiplicity, metadata.max_cycles)
}
""".strip()


def metadata_insert_sql(metadata: LoadedMetadata) -> str:
    return f"""
INSERT INTO {METADATA_TABLE}
SELECT
    toUInt64({metadata.schema_version}),
    '{metadata.generator_signature}',
    toUInt64({metadata.max_cardinality}),
    toUInt64({metadata.bucket_width}),
    toUInt64({metadata.max_multiplicity}),
    toUInt64({metadata.max_cycles}),
    toUInt64({metadata.max_build_payload_columns}),
    toUInt64({metadata.max_probe_payload_columns}),
    '{metadata.build_part_fingerprint}',
    '{metadata.probe_part_fingerprint}'
""".strip()


def part_fingerprint(pairs: Sequence[tuple[str, str]]) -> str:
    hasher = hashlib.sha256()
    for partition_id, part_hash in sorted(pairs):
        hasher.update(f"{partition_id}\t{part_hash}\n".encode("utf-8"))
    return hasher.hexdigest()


def parts_query() -> str:
    names = ", ".join(f"'{name}'" for name in (BUILD_TABLE, PROBE_TABLE))
    return (
        "SELECT table, partition_id, toString(hash_of_all_files) AS part_hash "
        "FROM system.parts WHERE database = currentDatabase() AND active "
        f"AND table IN ({names}) ORDER BY table, partition_id FORMAT JSONEachRow"
    )


def collect_part_fingerprints(
    rows: Sequence[dict[str, object]], metadata: LoadedMetadata
) -> tuple[str, str]:
    grouped: dict[str, list[tuple[str, str]]] = {
        BUILD_TABLE: [],
        PROBE_TABLE: [],
    }
    seen: set[tuple[str, str]] = set()
    for row in rows:
        try:
            table = str(row["table"])
            partition_id = str(row["partition_id"])
            part_hash = str(row["part_hash"])
        except KeyError as ex:
            raise ValueError(f"malformed part row: missing {ex.args[0]}") from ex
        if table not in grouped:
            raise ValueError(f"unexpected part table {table}")
        key = (table, partition_id)
        if key in seen:
            raise ValueError(f"multiple active parts in {table} partition {partition_id}")
        seen.add(key)
        grouped[table].append((partition_id, part_hash))

    expected = {
        BUILD_TABLE: {str(i) for i in range(metadata.max_multiplicity)},
        PROBE_TABLE: {str(i) for i in range(metadata.max_cycles)},
    }
    for table, pairs in grouped.items():
        actual = {partition_id for partition_id, _ in pairs}
        if actual != expected[table]:
            raise ValueError(
                f"{table} partitions {sorted(actual)} do not match expected "
                f"{sorted(expected[table])}"
            )
    return (
        part_fingerprint(grouped[BUILD_TABLE]),
        part_fingerprint(grouped[PROBE_TABLE]),
    )


def table_existence_query() -> str:
    names = ", ".join(f"'{name}'" for name in (BUILD_TABLE, PROBE_TABLE, METADATA_TABLE))
    return (
        "SELECT name FROM system.tables "
        f"WHERE database = currentDatabase() AND name IN ({names}) "
        "ORDER BY name FORMAT JSONEachRow"
    )


def metadata_query() -> str:
    return (
        f"SELECT schema_version, generator_signature, max_cardinality, bucket_width, "
        f"max_multiplicity, max_cycles, max_build_payload_columns, "
        f"max_probe_payload_columns, build_part_fingerprint, probe_part_fingerprint "
        f"FROM {METADATA_TABLE} FORMAT JSONEachRow"
    )


def _loaded_layout_select() -> str:
    names = ", ".join(f"'{name}'" for name in (BUILD_TABLE, PROBE_TABLE, METADATA_TABLE))
    return f"""
SELECT 'table' AS kind, name AS key,
       concat(engine, '|', partition_key, '|', sorting_key) AS value
FROM system.tables
WHERE database = currentDatabase() AND name IN ({names})
UNION ALL
SELECT 'column' AS kind, concat(table, '.', toString(position)) AS key,
       concat(name, '|', type) AS value
FROM system.columns
WHERE database = currentDatabase() AND table IN ({names})
UNION ALL
SELECT 'part' AS kind, concat(table, '.', partition_id) AS key,
       concat(toString(count()), '|', toString(any(hash_of_all_files))) AS value
FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ({names})
GROUP BY table, partition_id
""".strip()


def loaded_layout_query() -> str:
    return _loaded_layout_select() + "\nFORMAT JSONEachRow"


def loaded_state_query() -> str:
    return f"""
{_loaded_layout_select()}
UNION ALL
SELECT 'build_count' AS kind, toString(occurrence) AS key,
       toString(count()) AS value
FROM {BUILD_TABLE}
GROUP BY occurrence
UNION ALL
SELECT 'probe_count' AS kind, toString(cycle) AS key,
       toString(count()) AS value
FROM {PROBE_TABLE}
GROUP BY cycle
FORMAT JSONEachRow
""".strip()


def _expected_columns(table: str, columns: Sequence[tuple[str, str]]) -> dict[str, str]:
    return {
        f"{table}.{position}": f"{name}|{type_name}"
        for position, (name, type_name) in enumerate(columns, 1)
    }


def _group_state_rows(
    rows: Sequence[dict[str, object]],
) -> tuple[dict[str, dict[str, str]], list[str]]:
    grouped: dict[str, dict[str, str]] = {}
    for row in rows:
        try:
            kind = str(row["kind"])
            key = str(row["key"])
            value = str(row["value"])
        except KeyError as ex:
            return grouped, [f"malformed state row: missing {ex.args[0]}"]
        if key in grouped.setdefault(kind, {}):
            return grouped, [f"duplicate state row: {kind}/{key}"]
        grouped[kind][key] = value
    return grouped, []


def _validate_layout_maps(
    grouped: dict[str, dict[str, str]], metadata: LoadedMetadata
) -> list[str]:
    errors: list[str] = []
    expected_tables = {
        BUILD_TABLE: "MergeTree|occurrence|occurrence, card_bucket, shuffle_rank",
        PROBE_TABLE: "MergeTree|cycle|cycle, card_bucket, rank",
        METADATA_TABLE: "MergeTree||",
    }
    if grouped.get("table", {}) != expected_tables:
        errors.append("table engines or sorting keys do not match")

    expected_columns = {}
    expected_columns.update(
        _expected_columns(
            BUILD_TABLE, build_columns(metadata.max_build_payload_columns)
        )
    )
    expected_columns.update(
        _expected_columns(
            PROBE_TABLE, probe_columns(metadata.max_probe_payload_columns)
        )
    )
    expected_columns.update(_expected_columns(METADATA_TABLE, METADATA_COLUMNS))
    if grouped.get("column", {}) != expected_columns:
        errors.append("table columns do not match")

    parts = grouped.get("part", {})
    expected_build_parts = {
        f"{BUILD_TABLE}.{partition}"
        for partition in range(metadata.max_multiplicity)
    }
    expected_probe_parts = {
        f"{PROBE_TABLE}.{partition}" for partition in range(metadata.max_cycles)
    }
    metadata_parts = {
        key for key in parts if key.startswith(f"{METADATA_TABLE}.")
    }
    if (
        {key for key in parts if key.startswith(f"{BUILD_TABLE}.")}
        != expected_build_parts
        or {key for key in parts if key.startswith(f"{PROBE_TABLE}.")}
        != expected_probe_parts
        or len(metadata_parts) != 1
        or set(parts) != expected_build_parts | expected_probe_parts | metadata_parts
        or any(
            not value.startswith("1|") or not value.removeprefix("1|")
            for value in parts.values()
        )
    ):
        errors.append("tables do not have exactly one active part per partition")
    return errors


def _validate_metadata_layout(grouped: dict[str, dict[str, str]]) -> list[str]:
    errors: list[str] = []
    if grouped.get("table", {}).get(METADATA_TABLE) != "MergeTree||":
        errors.append("metadata table engine or sorting key does not match")
    expected_columns = _expected_columns(METADATA_TABLE, METADATA_COLUMNS)
    actual_columns = {
        key: value
        for key, value in grouped.get("column", {}).items()
        if key.startswith(f"{METADATA_TABLE}.")
    }
    if actual_columns != expected_columns:
        errors.append("metadata table columns do not match")
    metadata_parts = {
        key: value
        for key, value in grouped.get("part", {}).items()
        if key.startswith(f"{METADATA_TABLE}.")
    }
    metadata_part = next(iter(metadata_parts.values()), "")
    if (
        len(metadata_parts) != 1
        or not metadata_part.startswith("1|")
        or not metadata_part.removeprefix("1|")
    ):
        errors.append("metadata table does not have exactly one active part")
    return errors


def validate_loaded_state(rows: Sequence[dict[str, object]], metadata: LoadedMetadata) -> list[str]:
    grouped, errors = _group_state_rows(rows)
    if errors:
        return errors
    errors = _validate_layout_maps(grouped, metadata)
    parts = grouped.get("part", {})
    actual_fingerprints: dict[str, str] = {}
    for table in (BUILD_TABLE, PROBE_TABLE):
        prefix = f"{table}."
        part_pairs = [
            (key.removeprefix(prefix), value.partition("|")[2])
            for key, value in parts.items()
            if key.startswith(prefix)
        ]
        actual_fingerprints[table] = part_fingerprint(part_pairs)
    if (
        not metadata.build_part_fingerprint
        or not metadata.probe_part_fingerprint
        or actual_fingerprints[BUILD_TABLE] != metadata.build_part_fingerprint
        or actual_fingerprints[PROBE_TABLE] != metadata.probe_part_fingerprint
    ):
        errors.append("active part fingerprints do not match metadata")

    expected_build_partitions = {
        str(partition): str(metadata.max_cardinality)
        for partition in range(metadata.max_multiplicity)
    }
    expected_probe_partitions = {
        str(partition): str(metadata.max_cardinality)
        for partition in range(metadata.max_cycles)
    }
    if grouped.get("build_count", {}) != expected_build_partitions:
        errors.append("build per-partition counts do not match")
    if grouped.get("probe_count", {}) != expected_probe_partitions:
        errors.append("probe per-partition counts do not match")
    build_total = sum(int(value) for value in grouped.get("build_count", {}).values())
    probe_total = sum(int(value) for value in grouped.get("probe_count", {}).values())
    if build_total != metadata.max_cardinality * metadata.max_multiplicity:
        errors.append("build total row count does not match")
    if probe_total != metadata.max_cardinality * metadata.max_cycles:
        errors.append("probe total row count does not match")
    return errors


def validate_loaded_layout(
    rows: Sequence[dict[str, object]], metadata: LoadedMetadata
) -> list[str]:
    grouped, errors = _group_state_rows(rows)
    if errors:
        return errors
    return _validate_layout_maps(grouped, metadata)


def _settings(algorithm: str, threads: int, max_memory: int) -> str:
    if algorithm not in ALGORITHMS:
        raise ValueError(f"unsupported join algorithm: {algorithm}")
    return (
        f"SETTINGS join_algorithm = '{algorithm}', "
        f"max_threads = {threads}, "
        "query_plan_join_swap_table = false, "
        "enable_analyzer = 1, "
        "enable_join_runtime_filters = 0, "
        "max_bytes_before_external_join = 0, "
        "max_bytes_ratio_before_external_join = 0, "
        f"max_memory_usage = {max_memory}"
    )


def _projected_payloads(prefix: str, count: int, qualifier: str = "") -> list[str]:
    return [f"{qualifier}{prefix}{index}" for index in range(count)]


def _probe_selection(point: BenchmarkPoint) -> tuple[int, int, int, int, int]:
    if point.cardinality % point.bucket_width:
        raise ValueError("point cardinality must be divisible by bucket width")
    bucket_count = point.cardinality // point.bucket_width
    full_cycles, remainder = divmod(point.probe_rows, point.cardinality)
    full_buckets, remaining_ranks = divmod(remainder, point.bucket_width)
    return (
        full_cycles,
        remainder,
        full_buckets,
        remaining_ranks,
        bucket_count,
    )


def _probe_predicate(point: BenchmarkPoint) -> str:
    full_cycles, remainder, full_buckets, remaining_ranks, bucket_count = (
        _probe_selection(point)
    )
    first = f"(cycle < {full_cycles} AND card_bucket < {bucket_count})"
    if remainder == 0:
        return first
    if remaining_ranks == 0:
        second = f"(cycle = {full_cycles} AND card_bucket < {full_buckets})"
    elif full_buckets == 0:
        second = (
            f"(cycle = {full_cycles} AND "
            f"(card_bucket = 0 AND rank < {remaining_ranks}))"
        )
    else:
        second = (
            f"(cycle = {full_cycles} AND (card_bucket < {full_buckets} OR "
            f"(card_bucket = {full_buckets} AND rank < {remaining_ranks})))"
        )
    if full_cycles == 0:
        return second
    return f"{first} OR {second}"


def _probe_subquery(
    point: BenchmarkPoint, payload_columns: int | None = None
) -> str:
    count = (
        point.probe_payload_columns
        if payload_columns is None
        else payload_columns
    )
    payloads = _projected_payloads("p_p", count)
    select_suffix = "".join(f",\n    {name}" for name in payloads)
    dense = (
        f"cycle * {point.cardinality} + "
        f"card_bucket * {point.bucket_width} + rank"
    )
    predicate = _probe_predicate(point)
    return f"""
SELECT
    if(
        intDiv(toUInt128({dense} + 1) * {point.hit_rows}, {point.probe_rows})
            > intDiv(toUInt128({dense}) * {point.hit_rows}, {point.probe_rows}),
        hit_k,
        miss_k
    ) AS k{select_suffix}
FROM {PROBE_TABLE}
PREWHERE {predicate}
""".strip()


def _build_subquery(
    point: BenchmarkPoint, payload_columns: int | None = None
) -> str:
    count = (
        point.build_payload_columns
        if payload_columns is None
        else payload_columns
    )
    projection = ", ".join(["k"] + _projected_payloads("b_p", count))
    bucket_count = point.cardinality // point.bucket_width
    return f"""
SELECT {projection}
FROM {BUILD_TABLE}
PREWHERE occurrence < {point.multiplicity} AND card_bucket < {bucket_count}
""".strip()


def _join_from(
    point: BenchmarkPoint,
    build_payload_columns: int | None = None,
    probe_payload_columns: int | None = None,
) -> str:
    return (
        f"FROM ({_probe_subquery(point, probe_payload_columns)}) AS p "
        f"INNER JOIN ({_build_subquery(point, build_payload_columns)}) AS b USING (k)"
    )


def join_query(
    point: BenchmarkPoint,
    algorithm: str,
    threads: int,
    max_memory: int,
    *,
    output_format: str,
    order_by_all: bool = False,
) -> str:
    if output_format not in ("Null", "Hash"):
        raise ValueError(f"unsupported output format: {output_format}")
    order = " ORDER BY ALL" if order_by_all else ""
    projected = (
        _projected_payloads("p_p", point.probe_payload_columns, "p.")
        + _projected_payloads("b_p", point.build_payload_columns, "b.")
    )
    projection = ", ".join(projected) if projected else "toUInt8(0) AS matched"
    return (
        f"SELECT {projection} {_join_from(point)}{order} "
        f"{_settings(algorithm, threads, max_memory)} FORMAT {output_format}"
    )


def verification_query(
    point: BenchmarkPoint, algorithm: str, threads: int, max_memory: int
) -> str:
    return join_query(
        point,
        algorithm,
        threads,
        max_memory,
        output_format="Hash",
        order_by_all=True,
    )


def assertion_query(point: BenchmarkPoint, threads: int, max_memory: int) -> str:
    probe = _probe_subquery(point, 0)
    build = _build_subquery(point, 0)
    joined = (
        f"SELECT count() AS joined_count {_join_from(point, 0, 0)} "
        f"{_settings('parallel_hash', threads, max_memory)}"
    )
    return f"""
SELECT
    '{ASSERT_MARKER}',
    probe_count,
    build_count,
    joined_count
FROM (SELECT count() AS probe_count FROM ({probe}) AS probe_rows) AS probe_count_source
CROSS JOIN (SELECT count() AS build_count FROM ({build}) AS build_rows) AS build_count_source
CROSS JOIN ({joined}) AS joined_count_source
FORMAT TabSeparatedRaw
""".strip()


def measurement_script(
    point: BenchmarkPoint,
    algorithm: str,
    threads: int,
    max_memory: int,
    *,
    runs: int,
) -> str:
    if runs <= 0:
        raise ValueError("runs must be positive")
    timed = join_query(
        point, algorithm, threads, max_memory, output_format="Null"
    )
    statements = [timed]  # one warmup, followed by query-scoped timed packets
    statements.extend(timed for _ in range(runs))
    return ";\n".join(statements) + ";\n"


def parse_assertion_output(output: bytes) -> tuple[int, int, int]:
    matches = []
    for line in output.decode("utf-8", "strict").splitlines():
        fields = line.split("\t")
        if fields and fields[0] == ASSERT_MARKER:
            matches.append(fields)
    if len(matches) != 1 or len(matches[0]) != 4:
        raise ValueError("assertion output did not contain exactly one valid marker")
    try:
        values = tuple(int(value) for value in matches[0][1:])
    except ValueError as ex:
        raise ValueError("assertion marker contains a non-integer value") from ex
    if any(value < 0 for value in values):
        raise ValueError("assertion marker contains a negative value")
    return values  # type: ignore[return-value]


_PROFILE_EVENT_PREFIX_RE = re.compile(
    r"^.*\[\s*\d+\s*\]\s+(?P<event>[A-Za-z][A-Za-z0-9]*):"
)
_PROFILE_EVENT_LINE_RE = re.compile(
    r"^.*\[\s*\d+\s*\]\s+(?P<event>[A-Za-z][A-Za-z0-9]*):\s+"
    r"(?P<value>-?\d+)\s+\((?P<kind>increment|gauge)\)\s*$"
)
_WALL_TIME_LINE_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?$")


def parse_profile_events(
    stderr: str, *, expected_packets: int
) -> list[dict[str, int]]:
    """Parse final ProfileEvents packets paired with following `--time` lines."""
    if expected_packets <= 0:
        raise ValueError("expected packet count must be positive")
    packets: list[dict[str, int]] = []
    current: dict[str, int] | None = None
    current_seen: set[str] | None = None
    query_seen = False
    tracked = set(EVENTS)

    for raw_line in stderr.splitlines():
        line = raw_line.strip()
        if _WALL_TIME_LINE_RE.fullmatch(line):
            if current is None or current_seen is None:
                raise ValueError(f"unexpected wall-time line: {line}")
            if not query_seen:
                raise ValueError("profile-event packet is missing `Query`")
            elapsed_seconds = decimal.Decimal(line)
            if elapsed_seconds < 0:
                raise ValueError("wall time must be nonnegative")
            current[WALL_TIME_EVENT] = round_hit_count(
                1_000_000, elapsed_seconds
            )
            packets.append(current)
            current = None
            current_seen = None
            query_seen = False
            continue

        prefix_match = _PROFILE_EVENT_PREFIX_RE.match(line)
        if not prefix_match:
            continue
        event_name = prefix_match.group("event")
        line_match = _PROFILE_EVENT_LINE_RE.match(line)
        if not line_match:
            if event_name == "Query" or event_name in tracked:
                raise ValueError(f"malformed profile-event line: {line}")
            continue

        if current is None:
            current = dict.fromkeys(EVENTS, 0)
            current_seen = set()
        value = int(line_match.group("value"))
        kind = line_match.group("kind")
        if event_name == "Query":
            if value != 1 or kind != "increment":
                raise ValueError("profile packet must contain `Query: 1 (increment)`")
            if query_seen:
                raise ValueError("duplicate profile event Query in one packet")
            query_seen = True
            continue
        if event_name not in tracked:
            continue
        assert current_seen is not None
        expected_kind = "gauge" if event_name in GAUGE_EVENTS else "increment"
        if kind != expected_kind:
            raise ValueError(f"profile event {event_name} is not a {expected_kind}")
        if value < 0:
            raise ValueError(f"profile event {event_name} is negative")
        if event_name in current_seen:
            raise ValueError(f"duplicate profile event {event_name} in one packet")
        current_seen.add(event_name)
        current[event_name] = value

    if current is not None:
        raise ValueError("profile-event packet is missing its wall-time line")
    if len(packets) != expected_packets:
        raise ValueError(
            f"expected {expected_packets} profile-event packets, got {len(packets)}"
        )
    return packets


def parse_timed_profile_events(stderr: str, *, runs: int) -> list[dict[str, int]]:
    if runs <= 0:
        raise ValueError("runs must be positive")
    packets = parse_profile_events(stderr, expected_packets=runs + 1)
    return packets[1:]


def summarize_measurements(runs: Sequence[dict[str, int]]) -> Measurements:
    if not runs:
        raise ValueError("no timed runs")
    elapsed = [run[WALL_TIME_EVENT] for run in runs]
    if any(value <= 0 for value in elapsed):
        raise ValueError("every timed run must have positive wall time")
    median_us = statistics.median(elapsed)
    representative = min(
        range(len(runs)), key=lambda index: (abs(elapsed[index] - median_us), index)
    )
    return Measurements(
        median_us=median_us,
        min_us=min(elapsed),
        events=dict(runs[representative]),
    )


def fallback_reason(algorithm: str, runs: Sequence[dict[str, int]]) -> str | None:
    build_event = "RadixHashJoinBuildMicroseconds"
    probe_event = "RadixHashJoinProbeMicroseconds"
    leaf_builds_event = "RadixHashJoinLeafGroupBuilds"
    for index, values in enumerate(runs, 1):
        build = values[build_event]
        probe = values[probe_event]
        leaf_builds = values[leaf_builds_event]
        if algorithm == "radix_join":
            if leaf_builds == 0:
                return (
                    f"run {index}: radix leaf-group build count must be nonzero "
                    f"(leaf_builds={leaf_builds})"
                )
        elif algorithm == "parallel_hash":
            if leaf_builds != 0 or build != 0 or probe != 0:
                return (
                    f"run {index}: radix path events must all be zero "
                    f"(leaf_builds={leaf_builds}, build={build}, probe={probe})"
                )
        else:
            raise ValueError(f"unsupported join algorithm: {algorithm}")
    return None


def _run_local(
    binary: str,
    path: str,
    sql: str,
    *,
    timeout: float | None = None,
    profile_events: bool = False,
) -> tuple[int | None, bytes, str]:
    command = [binary, "local", f"--path={path}", "--multiquery"]
    if profile_events:
        command.extend(
            [
                "--print-profile-events",
                "--time",
                "--progress=off",
                "--profile-events-delay-ms=-1",
            ]
        )
    try:
        process = subprocess.run(
            command,
            input=sql.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as ex:
        stdout = ex.stdout if isinstance(ex.stdout, bytes) else b""
        return None, stdout, f"timed out after {timeout} seconds"
    except OSError as ex:
        return None, b"", str(ex)
    return (
        process.returncode,
        process.stdout,
        process.stderr.decode("utf-8", "replace").strip(),
    )


def _query_json(
    binary: str, path: str, sql: str, *, timeout: float | None = None
) -> list[dict[str, object]]:
    returncode, stdout, stderr = _run_local(binary, path, sql, timeout=timeout)
    if returncode != 0:
        raise RuntimeError(
            f"`clickhouse local` failed (rc={returncode}): {stderr or 'no diagnostic'}"
        )
    rows = []
    for line in stdout.decode("utf-8", "strict").splitlines():
        if line.strip():
            value = json.loads(line)
            if not isinstance(value, dict):
                raise RuntimeError("expected JSON object from `clickhouse local`")
            rows.append(value)
    return rows


def _table_names(binary: str, path: str) -> set[str]:
    return {str(row["name"]) for row in _query_json(binary, path, table_existence_query())}


def read_metadata(binary: str, path: str) -> LoadedMetadata | None:
    required = {BUILD_TABLE, PROBE_TABLE, METADATA_TABLE}
    if _table_names(binary, path) != required:
        return None
    layout_rows = _query_json(binary, path, loaded_layout_query())
    grouped, grouping_errors = _group_state_rows(layout_rows)
    if grouping_errors or _validate_metadata_layout(grouped):
        return None
    rows = _query_json(binary, path, metadata_query())
    if len(rows) != 1:
        return None
    row = rows[0]
    try:
        metadata = LoadedMetadata(
            schema_version=int(str(row["schema_version"])),
            generator_signature=str(row["generator_signature"]),
            max_cardinality=int(str(row["max_cardinality"])),
            bucket_width=int(str(row["bucket_width"])),
            max_multiplicity=int(str(row["max_multiplicity"])),
            max_cycles=int(str(row["max_cycles"])),
            max_build_payload_columns=int(str(row["max_build_payload_columns"])),
            max_probe_payload_columns=int(str(row["max_probe_payload_columns"])),
            build_part_fingerprint=str(row["build_part_fingerprint"]),
            probe_part_fingerprint=str(row["probe_part_fingerprint"]),
        )
        validate_load_dimensions(metadata)
    except (KeyError, TypeError, ValueError):
        return None
    if validate_loaded_layout(layout_rows, metadata):
        return None
    return metadata


def inspect_loaded_data(binary: str, path: str, metadata: LoadedMetadata) -> list[str]:
    layout_rows = _query_json(binary, path, loaded_layout_query())
    layout_errors = validate_loaded_layout(layout_rows, metadata)
    if layout_errors:
        return layout_errors
    rows = _query_json(binary, path, loaded_state_query())
    return validate_loaded_state(rows, metadata)


def _metadata_matches(left: LoadedMetadata, right: LoadedMetadata) -> bool:
    return (
        left.schema_version == right.schema_version
        and left.max_cardinality == right.max_cardinality
        and left.bucket_width == right.bucket_width
        and left.max_multiplicity == right.max_multiplicity
        and left.max_cycles == right.max_cycles
        and left.max_build_payload_columns == right.max_build_payload_columns
        and left.max_probe_payload_columns == right.max_probe_payload_columns
        and left.generator_signature == right.generator_signature
    )


def _validate_binary(binary: str) -> None:
    if not os.path.isfile(binary):
        raise ValueError(f"binary not found: {binary}")
    if not os.access(binary, os.X_OK):
        raise ValueError(f"binary is not executable: {binary}")


def _parse_load_metadata(args: argparse.Namespace) -> LoadedMetadata:
    metadata = LoadedMetadata(
        schema_version=SCHEMA_VERSION,
        max_cardinality=args.max_cardinality,
        bucket_width=args.bucket_width,
        max_multiplicity=args.max_multiplicity,
        max_cycles=args.max_cycles,
        max_build_payload_columns=args.max_build_payload_columns,
        max_probe_payload_columns=args.max_probe_payload_columns,
        generator_signature=GENERATOR_SIGNATURE,
    )
    validate_load_dimensions(metadata)
    return metadata


def load_command(args: argparse.Namespace) -> int:
    try:
        _validate_binary(args.binary)
        expected = _parse_load_metadata(args)
        pathlib.Path(args.path).mkdir(parents=True, exist_ok=True)
        raw_bytes = estimate_raw_bytes(expected)
        print(f"Estimated raw data size: {raw_bytes} bytes.")
        actual = read_metadata(args.binary, args.path)
        if actual is not None and _metadata_matches(actual, expected):
            errors = inspect_loaded_data(args.binary, args.path, actual)
            if not errors:
                print(
                    f"READY: {args.path} already contains schema version {SCHEMA_VERSION} "
                    "with matching parameters and validated data; no changes made."
                )
                return 0
            print("RECREATE: matching metadata but invalid data: " + "; ".join(errors))
        elif actual is None:
            print("RECREATE: benchmark tables or valid metadata are absent.")
        else:
            print("RECREATE: loaded metadata does not match requested parameters.")

        free_bytes = _filesystem_free_bytes(args.path)
        print(f"Target filesystem free space: {free_bytes} bytes.")
        validate_load_capacity(expected, free_bytes=free_bytes)

        statements = [
            recreate_schema_sql(
                expected.max_build_payload_columns,
                expected.max_probe_payload_columns,
            )
        ]
        statements.append(build_insert_sql(expected))
        statements.append(probe_insert_sql(expected))
        statements.extend(
            [
                f"OPTIMIZE TABLE {BUILD_TABLE} FINAL",
                f"OPTIMIZE TABLE {PROBE_TABLE} FINAL",
            ]
        )
        returncode, _, stderr = _run_local(
            args.binary, args.path, ";\n".join(statements) + ";\n"
        )
        if returncode != 0:
            print(
                f"ERROR: loading failed (rc={returncode}): {stderr or 'no diagnostic'}",
                file=sys.stderr,
            )
            return 1

        build_part_fingerprint, probe_part_fingerprint = collect_part_fingerprints(
            _query_json(args.binary, args.path, parts_query()),
            expected,
        )
        loaded = dataclasses.replace(
            expected,
            build_part_fingerprint=build_part_fingerprint,
            probe_part_fingerprint=probe_part_fingerprint,
        )
        returncode, _, stderr = _run_local(
            args.binary, args.path, metadata_insert_sql(loaded) + ";\n"
        )
        if returncode != 0:
            print(
                f"ERROR: metadata write failed (rc={returncode}): "
                f"{stderr or 'no diagnostic'}",
                file=sys.stderr,
            )
            return 1

        actual = read_metadata(args.binary, args.path)
        if actual is None or not _metadata_matches(actual, expected):
            print("ERROR: metadata validation failed after load", file=sys.stderr)
            return 1
        errors = inspect_loaded_data(args.binary, args.path, actual)
        if errors:
            print("ERROR: post-load validation failed: " + "; ".join(errors), file=sys.stderr)
            return 1
        print(
            f"LOADED: {args.path}; D_max={expected.max_cardinality} "
            f"w={expected.bucket_width} "
            f"max_multiplicity={expected.max_multiplicity} "
            f"max_cycles={expected.max_cycles} "
            f"max_build_payload_columns={expected.max_build_payload_columns} "
            f"max_probe_payload_columns={expected.max_probe_payload_columns}."
        )
        return 0
    except (ValueError, RuntimeError, json.JSONDecodeError, UnicodeDecodeError) as ex:
        print(f"ERROR: {ex}", file=sys.stderr)
        return 2


def _execute_bytes(
    binary: str, path: str, sql: str, *, purpose: str
) -> tuple[bytes | None, str | None]:
    returncode, stdout, stderr = _run_local(binary, path, sql)
    if returncode != 0:
        return None, f"{purpose} failed (rc={returncode}): {stderr or 'no diagnostic'}"
    return stdout, None


def _assert_point(
    binary: str, path: str, point: BenchmarkPoint, threads: int, max_memory: int
) -> str | None:
    stdout, error = _execute_bytes(
        binary,
        path,
        assertion_query(point, threads, max_memory),
        purpose="untimed assertion",
    )
    if error:
        return error
    try:
        actual = parse_assertion_output(stdout or b"")
    except (ValueError, UnicodeDecodeError) as ex:
        return f"could not parse untimed assertion: {ex}"
    expected = (point.probe_rows, point.build_rows, point.output_rows)
    if actual != expected:
        return (
            f"counts probe/build/joined={actual[0]}/{actual[1]}/{actual[2]}, "
            f"expected={expected[0]}/{expected[1]}/{expected[2]}"
        )
    return None


def _verify_point(
    binary: str,
    path: str,
    point: BenchmarkPoint,
    threads: int,
    max_memory: int,
    no_verify: bool,
    verify_max_output_rows: int,
) -> tuple[str, str, dict[str, str]]:
    if no_verify:
        return "SKIP", "disabled", {}
    if point.output_rows > verify_max_output_rows:
        return (
            "SKIP",
            f"output rows {point.output_rows} exceed cap {verify_max_output_rows}",
            {},
        )
    hashes: dict[str, bytes] = {}
    errors: dict[str, str] = {}
    for algorithm in ALGORITHMS:
        stdout, error = _execute_bytes(
            binary,
            path,
            verification_query(point, algorithm, threads, max_memory),
            purpose=f"{algorithm} hash verification",
        )
        if error:
            errors[algorithm] = error
            continue
        hashes[algorithm] = stdout or b""
    if errors:
        return (
            "ERROR",
            "; ".join(f"{algorithm}: {error}" for algorithm, error in errors.items()),
            errors,
        )
    if hashes["radix_join"] != hashes["parallel_hash"]:
        return "FAIL", "FORMAT Hash mismatch", {}
    return "PASS", "identical sorted output", {}


def _measure_algorithm(
    binary: str,
    path: str,
    point: BenchmarkPoint,
    algorithm: str,
    threads: int,
    max_memory: int,
    runs: int,
) -> AlgorithmResult:
    returncode, _, stderr = _run_local(
        binary,
        path,
        measurement_script(point, algorithm, threads, max_memory, runs=runs),
        profile_events=True,
    )
    if returncode != 0:
        return AlgorithmResult(
            algorithm=algorithm,
            status="ERROR",
            detail=(
                f"{algorithm} measurement failed (rc={returncode}): "
                f"{stderr or 'no diagnostic'}"
            ),
        )
    try:
        event_runs = parse_timed_profile_events(stderr, runs=runs)
        measurements = summarize_measurements(event_runs)
        fallback = fallback_reason(algorithm, event_runs)
    except (ValueError, KeyError) as ex:
        return AlgorithmResult(
            algorithm=algorithm, status="ERROR", detail=f"event parsing failed: {ex}"
        )
    if fallback:
        return AlgorithmResult(
            algorithm=algorithm,
            status="FALLBACK",
            measurements=measurements,
            detail=fallback,
        )
    return AlgorithmResult(
        algorithm=algorithm, status="OK", measurements=measurements
    )


def _ms(value_us: int | float) -> str:
    return f"{float(value_us) / 1000.0:.3f}"


def _event_ms(events: dict[str, int], name: str) -> str:
    return _ms(events[name])


def _event_mb(events: dict[str, int], name: str) -> str:
    return f"{events[name] / (1024 * 1024):.3f}"


def _print_point_results(
    point: BenchmarkPoint,
    verification: tuple[str, str],
    results: Sequence[AlgorithmResult],
    verification_errors: dict[str, str] | None = None,
) -> tuple[str | None, float | None]:
    verification_errors = verification_errors or {}
    print(f"\nPoint: {point.label} N_p={point.probe_rows} n_hit={point.hit_rows}")
    print(f"Verification: {verification[0]} ({verification[1]})")
    headers = (
        "algorithm",
        "status",
        "verify",
        "median_ms",
        "min_ms",
        "build_ms",
        "probe_ms",
        "collect_ms",
        "pack_ms",
        "leaf_builds",
        "leaf_ms",
        "hash_match_ms",
        "hash_gather_ms",
        "dispatch_ms",
        "selected_rows",
        "selected_bytes",
        "peak_mem_mb",
    )
    rows: list[tuple[str, ...]] = []
    for result in results:
        verify_status = (
            "ERROR"
            if result.algorithm in verification_errors
            else "PASS"
            if verification[0] == "ERROR"
            else verification[0]
        )
        if result.measurements is None:
            rows.append(
                (result.algorithm, result.status, verify_status)
                + ("-",) * (len(headers) - 3)
            )
            continue
        events = result.measurements.events
        rows.append(
            (
                result.algorithm,
                result.status,
                verify_status,
                _ms(result.measurements.median_us),
                _ms(result.measurements.min_us),
                _event_ms(events, "RadixHashJoinBuildMicroseconds"),
                _event_ms(events, "RadixHashJoinProbeMicroseconds"),
                _event_ms(events, "RadixHashJoinProbeCollectMatchesMicroseconds"),
                _event_ms(events, "RadixHashJoinProbePackHashRouteMicroseconds"),
                str(events["RadixHashJoinLeafGroupBuilds"]),
                _event_ms(events, "RadixHashJoinLeafGroupBuildMicroseconds"),
                _event_ms(events, "HashJoinProbeMatchMicroseconds"),
                _event_ms(events, "HashJoinProbeGatherMicroseconds"),
                _event_ms(events, "ConcurrentHashJoinProbeDispatchMicroseconds"),
                str(events["SelectedRows"]),
                str(events["SelectedBytes"]),
                _event_mb(events, "MemoryTrackerPeakUsage"),
            )
        )
    widths = [
        max([len(headers[index])] + [len(row[index]) for row in rows])
        for index in range(len(headers))
    ]
    print("  ".join(value.ljust(widths[index]) for index, value in enumerate(headers)))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))
    for result in results:
        if result.detail:
            print(f"  {result.algorithm}: {result.detail}")

    if verification[0] in ("FAIL", "ERROR") or any(
        result.status != "OK" for result in results
    ):
        print("Winner: excluded")
        return None, None
    radix = next(result for result in results if result.algorithm == "radix_join")
    parallel = next(result for result in results if result.algorithm == "parallel_hash")
    assert radix.measurements is not None and parallel.measurements is not None
    radix_time = float(radix.measurements.median_us)
    parallel_time = float(parallel.measurements.median_us)
    if radix_time == parallel_time:
        print("Winner: tie (1.000x)")
        return "tie", 1.0
    if radix_time < parallel_time:
        speedup = parallel_time / radix_time
        print(f"Winner: radix_join ({speedup:.3f}x)")
        return "radix_join", speedup
    speedup = radix_time / parallel_time
    print(f"Winner: parallel_hash ({speedup:.3f}x)")
    return "parallel_hash", speedup


def _available_cpu_count() -> int:
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        return os.cpu_count() or 1


def run_command(args: argparse.Namespace) -> int:
    counts = {
        "wins": 0,
        "losses": 0,
        "ties": 0,
        "fallback": 0,
        "invalid": 0,
        "errors": 0,
        "hash_mismatch": 0,
    }
    try:
        _validate_binary(args.binary)
        multiplicities = parse_integer_list(args.multiplicities, "multiplicities")
        ratios = parse_decimal_list(args.ratios, "ratios")
        hit_rates = parse_decimal_list(args.hit_rates, "hit-rates", allow_zero=True)
        build_payload_columns = parse_nonnegative_integer_list(
            args.build_payload_columns, "build-payload-columns"
        )
        probe_payload_columns = parse_nonnegative_integer_list(
            args.probe_payload_columns, "probe-payload-columns"
        )
        if args.threads <= 0:
            raise ValueError("threads must be positive")
        if args.runs <= 0:
            raise ValueError("runs must be positive")
        if args.max_memory <= 0:
            raise ValueError("max-memory must be positive")
        if args.verify_max_output_rows < 0:
            raise ValueError("verify-max-output-rows must be nonnegative")

        metadata = read_metadata(args.binary, args.path)
        if metadata is None:
            raise ValueError("loaded metadata is missing or malformed; run `load` first")
        if metadata.schema_version != SCHEMA_VERSION:
            raise ValueError(
                f"loaded schema version {metadata.schema_version} != expected {SCHEMA_VERSION}"
            )
        if metadata.generator_signature != GENERATOR_SIGNATURE:
            raise ValueError(
                "loaded generator signature does not match this benchmark version"
            )
        cardinalities = (
            parse_integer_list(args.cardinalities, "cardinalities")
            if args.cardinalities
            else list(metadata.cardinalities)
        )
        points = validate_points(
            metadata,
            cardinalities,
            multiplicities,
            ratios,
            hit_rates,
            build_payload_columns,
            probe_payload_columns,
        )
        state_errors = inspect_loaded_data(args.binary, args.path, metadata)
        if state_errors:
            raise ValueError("loaded data is invalid: " + "; ".join(state_errors))
    except (ValueError, RuntimeError, json.JSONDecodeError, UnicodeDecodeError) as ex:
        print(f"ERROR: {ex}", file=sys.stderr)
        return 2

    print(
        f"Benchmark path={args.path} points={len(points)} threads={args.threads} "
        f"runs={args.runs} "
        f"build_payload_columns={','.join(map(str, build_payload_columns))} "
        f"probe_payload_columns={','.join(map(str, probe_payload_columns))}; "
        "n_hit=round-half-up(N_p * hit_rate)."
    )
    for point in points:
        assertion_error = _assert_point(
            args.binary, args.path, point, args.threads, args.max_memory
        )
        if assertion_error:
            counts["invalid"] += 1
            print(f"\nPoint: {point.label}")
            print(f"INVALID: {assertion_error}")
            continue
        print(
            f"\nAssertions: PASS for {point.label}; "
            f"probe/build/joined={point.probe_rows}/{point.build_rows}/{point.output_rows}"
        )

        verification_status, verification_detail, verification_errors = _verify_point(
            args.binary,
            args.path,
            point,
            args.threads,
            args.max_memory,
            args.no_verify,
            args.verify_max_output_rows,
        )
        verification = (verification_status, verification_detail)
        if verification_status == "FAIL":
            counts["hash_mismatch"] += 1
            _print_point_results(point, verification, [], verification_errors)
            continue

        results = []
        for algorithm in ALGORITHMS:
            if algorithm in verification_errors:
                results.append(
                    AlgorithmResult(
                        algorithm=algorithm,
                        status="ERROR",
                        detail=verification_errors[algorithm],
                    )
                )
            else:
                results.append(
                    _measure_algorithm(
                        args.binary,
                        args.path,
                        point,
                        algorithm,
                        args.threads,
                        args.max_memory,
                        args.runs,
                    )
                )
        if any(result.status == "FALLBACK" for result in results):
            counts["fallback"] += 1
        if any(result.status == "ERROR" for result in results):
            counts["errors"] += 1
        winner, _ = _print_point_results(
            point, verification, results, verification_errors
        )
        if winner == "radix_join":
            counts["wins"] += 1
        elif winner == "parallel_hash":
            counts["losses"] += 1
        elif winner == "tie":
            counts["ties"] += 1

    print(
        "\nSummary: "
        f"wins={counts['wins']} losses={counts['losses']} ties={counts['ties']} "
        f"fallback={counts['fallback']} invalid={counts['invalid']} "
        f"errors={counts['errors']} hash_mismatch={counts['hash_mismatch']}"
    )
    has_failures = any(
        counts[name] for name in ("fallback", "invalid", "errors", "hash_mismatch")
    )
    return 1 if has_failures else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Load and benchmark deterministic persistent `MergeTree` join data "
            "with `clickhouse local`."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    load = subparsers.add_parser(
        "load",
        help="idempotently create and validate persistent benchmark tables",
        description=(
            "Create deterministic build/probe `MergeTree` tables. Matching valid "
            "metadata is a no-op; any mismatch causes a fail-closed rebuild."
        ),
    )
    load.add_argument("--path", default=DEFAULT_PATH, help=f"data path (default: {DEFAULT_PATH})")
    load.add_argument(
        "--binary", default=DEFAULT_BINARY, help=f"`clickhouse` binary (default: {DEFAULT_BINARY})"
    )
    load.add_argument(
        "--max-cardinality",
        type=int,
        required=True,
        help="largest selectable distinct-key count D_max",
    )
    load.add_argument(
        "--bucket-width",
        type=int,
        required=True,
        help="power-of-two cardinality bucket width w dividing D_max",
    )
    load.add_argument(
        "--max-multiplicity",
        type=int,
        default=1,
        help="maximum build-key multiplicity, at most 64 (default: 1)",
    )
    load.add_argument(
        "--max-cycles",
        type=int,
        default=1,
        help="maximum probe cycles, at most 128 (default: 1)",
    )
    load.add_argument(
        "--max-build-payload-columns",
        type=int,
        default=1,
        help="maximum loaded build-side UInt64 payload columns (default: 1)",
    )
    load.add_argument(
        "--max-probe-payload-columns",
        type=int,
        default=1,
        help="maximum loaded probe-side UInt64 payload columns (default: 1)",
    )
    load.set_defaults(handler=load_command)

    run = subparsers.add_parser(
        "run",
        help="validate data, verify results, and benchmark both algorithms",
        description=(
            "Benchmark `radix_join` and `parallel_hash`. For N_p probe rows and "
            "hit rate h, n_hit is exact decimal round-half-up(N_p*h)."
        ),
    )
    run.add_argument("--path", default=DEFAULT_PATH, help=f"data path (default: {DEFAULT_PATH})")
    run.add_argument(
        "--binary", default=DEFAULT_BINARY, help=f"`clickhouse` binary (default: {DEFAULT_BINARY})"
    )
    run.add_argument(
        "--cardinalities",
        help=(
            "optional comma-separated multiples of loaded w in [w,D_max] "
            "(default: all selectable cardinalities)"
        ),
    )
    run.add_argument(
        "--multiplicities", required=True, help="comma-separated positive integer list"
    )
    run.add_argument(
        "--ratios",
        required=True,
        help="comma-separated positive decimal ratios; integral decimals such as 2.0 are allowed",
    )
    run.add_argument(
        "--hit-rates",
        default="1.0",
        help="comma-separated exact decimals in [0,1] (default: 1.0)",
    )
    run.add_argument(
        "--build-payload-columns",
        default="1",
        help=(
            "comma-separated nonnegative build payload column counts, each no "
            "greater than the loaded maximum (default: 1)"
        ),
    )
    run.add_argument(
        "--probe-payload-columns",
        default="1",
        help=(
            "comma-separated nonnegative probe payload column counts, each no "
            "greater than the loaded maximum (default: 1)"
        ),
    )
    run.add_argument(
        "--threads",
        type=int,
        default=_available_cpu_count(),
        help="max_threads for both algorithms (default: available CPU count)",
    )
    run.add_argument("--runs", type=int, default=3, help="timed runs after warmup (default: 3)")
    run.add_argument(
        "--max-memory",
        type=int,
        default=DEFAULT_MAX_MEMORY,
        help=f"max_memory_usage in bytes (default: {DEFAULT_MAX_MEMORY})",
    )
    run.add_argument(
        "--verify-max-output-rows",
        type=int,
        default=DEFAULT_VERIFY_MAX_OUTPUT_ROWS,
        help=(
            "skip sorted FORMAT Hash verification above this expected output "
            f"row count (default: {DEFAULT_VERIFY_MAX_OUTPUT_ROWS})"
        ),
    )
    run.add_argument(
        "--no-verify", action="store_true", help="skip cross-algorithm FORMAT Hash verification"
    )
    run.set_defaults(handler=run_command)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    sys.exit(main())

"""Open ClickHouse bugs the fuzzer would hit on every run, and the gates that steer around them.

A gate is checked by name where the generator or the oracle would otherwise walk into the bug
(`gates.active("...")`). While a gate is active the harness avoids the triggering shape (a table
option, a check, a type) instead of tolerating a wrong answer, so every other check stays strict.

Set `FOREIGN_WRITER_FUZZ_KNOWN_ISSUES=1` to open all gates, e.g. to check whether an issue has been
fixed. When an issue is closed, delete its entry here and the fuzzer starts guarding the fix.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class KnownIssue:
    gate: str
    url: str
    summary: str


KNOWN_ISSUES = [
    KnownIssue(
        "hudi_merge_on_read",
        "https://github.com/ClickHouse/ClickHouse/issues/114994",
        "Hudi Merge-on-Read log files are ignored; the Hudi writer creates Copy-on-Write tables only.",
    ),
    KnownIssue(
        "paimon_primary_key_tables",
        "https://github.com/ClickHouse/ClickHouse/issues/128549",
        "Paimon primary-key tables are refused with `NOT_IMPLEMENTED` because merge-on-read is not implemented (https://github.com/ClickHouse/ClickHouse/pull/113460); the Paimon writer creates append-only tables.",
    ),
    KnownIssue(
        "paimon_columns_by_name",
        "https://github.com/ClickHouse/ClickHouse/issues/128541",
        "Paimon columns are resolved by name instead of field id; since https://github.com/ClickHouse/ClickHouse/pull/127816 a data file written before a rename or a drop and re-add is refused with `NOT_IMPLEMENTED`; the Paimon writer does not rename or drop columns.",
    ),
    KnownIssue(
        "paimon_date_before_epoch",
        "https://github.com/ClickHouse/ClickHouse/issues/128306",
        "Paimon `DATE` is read as `Date` instead of `Date32`, so a date before 1970-01-01 fails with `VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE`; the Paimon writer does not use `DATE`.",
    ),
    KnownIssue(
        "paimon_blank_string_partition",
        "https://github.com/ClickHouse/ClickHouse/issues/127547",
        "ClickHouse builds the wrong directory (`c=`) for a blank string Paimon partition value and fails with `S3_ERROR` 404; the Paimon writer does not partition by strings.",
    ),
    KnownIssue(
        "paimon_row_type",
        "https://github.com/ClickHouse/ClickHouse/issues/128307",
        "A Paimon `ROW` (struct) column makes the table unreadable (`Unsupported Paimon type: ROW`, `Paimon/Types.h`); the Paimon writer does not use structs.",
    ),
    KnownIssue(
        "clickhouse_iceberg_compaction_after_schema_change",
        "https://github.com/ClickHouse/ClickHouse/issues/120833",
        "ClickHouse Iceberg compaction (`OPTIMIZE`) after a `DELETE` on a schema-evolved table rewrites the metadata under schema-id 0, which then fails with `ICEBERG_SPECIFICATION_VIOLATION`; the ClickHouse Iceberg writer does not compact.",
    ),
    KnownIssue(
        "clickhouse_iceberg_timestamp_ntz",
        "https://github.com/ClickHouse/ClickHouse/issues/114854",
        "ClickHouse writes Iceberg `timestamp` (without zone) columns as Parquet `isAdjustedToUTC = true`, which PyIceberg rejects; the ClickHouse Iceberg writer does not create `DateTime64` columns without a time zone.",
    ),
    KnownIssue(
        "clickhouse_iceberg_bucket_partition_type",
        "https://github.com/ClickHouse/ClickHouse/issues/125366",
        "ClickHouse writes Iceberg `bucket` partition values as `long` instead of `int`, so Spark never matches a ClickHouse position delete file that covers two data files of a partition and returns the rows that `DELETE` or `UPDATE` removed; the ClickHouse Iceberg writer does not partition by `bucket`.",
    ),
    KnownIssue(
        "clickhouse_iceberg_truncate_int_partition_type",
        "https://github.com/ClickHouse/ClickHouse/issues/118905",
        "ClickHouse writes Iceberg `truncate` partition values of an `int` column as `long` (`icebergTruncate` returns `Int64` for `Int32`), so Spark never matches a ClickHouse position delete file that covers two data files of a partition and returns the rows that `DELETE` or `UPDATE` removed; the ClickHouse Iceberg writer does not partition `int` columns by `truncate`.",
    ),
]


class Gates:
    def __init__(self, open_all=None):
        if open_all is None:
            open_all = os.environ.get("FOREIGN_WRITER_FUZZ_KNOWN_ISSUES", "0") == "1"
        self.open_all = open_all
        self._by_gate = {issue.gate: issue for issue in KNOWN_ISSUES}

    def active(self, gate):
        if gate not in self._by_gate:
            raise KeyError(f"Unknown known-issue gate {gate}")
        return not self.open_all

    def describe(self):
        if self.open_all:
            return "all known-issue gates are open"
        return "active gates: " + ", ".join(f"{i.gate} ({i.url})" for i in KNOWN_ISSUES)

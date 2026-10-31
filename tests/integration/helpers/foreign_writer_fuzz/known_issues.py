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
        "delta_trivial_count_with_deletion_vectors",
        "https://github.com/ClickHouse/ClickHouse/issues/114933",
        "Delta Lake trivial `count()` sums `numRecords` and ignores deletion vectors; the trivial count is checked with `optimize_trivial_count_query = 0` only on tables with deletion vectors.",
    ),
    KnownIssue(
        "hudi_merge_on_read",
        "https://github.com/ClickHouse/ClickHouse/issues/114994",
        "Hudi Merge-on-Read log files are ignored; the Hudi writer creates Copy-on-Write tables only.",
    ),
    KnownIssue(
        "hudi_replacecommit",
        "https://github.com/ClickHouse/ClickHouse/issues/123954",
        "Hudi `replacecommit` is ignored, so file groups replaced by `insert_overwrite` are still read; the Hudi writer does not overwrite.",
    ),
    KnownIssue(
        "paimon_schema_change_without_snapshot",
        "https://github.com/ClickHouse/ClickHouse/issues/128308",
        "Paimon `ALTER TABLE` writes a new schema without a new snapshot; ClickHouse keeps reading with the schema of the latest snapshot, so an added column is unknown until the next commit. The checks are skipped until the next snapshot.",
    ),
    KnownIssue(
        "paimon_columns_by_name",
        "https://github.com/ClickHouse/ClickHouse/issues/127545",
        "Paimon columns are resolved by name instead of field id, so values of renamed or dropped and re-added columns are wrong; the Paimon writer does not rename or drop columns.",
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
        "clickhouse_iceberg_v1_metadata",
        "https://github.com/ClickHouse/ClickHouse/issues/128301",
        "ClickHouse-written Iceberg format version 1 metadata lacks the required `schema` field, so PyIceberg cannot open the table; the ClickHouse Iceberg writer uses format version 2.",
    ),
    KnownIssue(
        "clickhouse_iceberg_bool",
        "https://github.com/ClickHouse/ClickHouse/issues/128303",
        "ClickHouse cannot create an Iceberg table with a `Bool` column (`Unsupported type for iceberg Bool`); the ClickHouse Iceberg writer does not use `Bool`.",
    ),
    KnownIssue(
        "clickhouse_iceberg_timestamp_ntz",
        "https://github.com/ClickHouse/ClickHouse/issues/114854",
        "ClickHouse writes Iceberg `timestamp` (without zone) columns as Parquet `isAdjustedToUTC = true`, which PyIceberg rejects; the ClickHouse Iceberg writer does not create `DateTime64` columns without a time zone.",
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

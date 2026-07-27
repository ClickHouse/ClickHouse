#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Part 1/5 of the Iceberg write/mutation/compaction schema-drift guard regression
# (issues #109835 / #109838): INSERT sink column-count mismatch and same-width rename drift.
# Part 2 (UPDATE/DELETE mutation) lives in 04369_iceberg_write_schema_drift_guards_mutation_109838.sh.
# Part 3 (OPTIMIZE compaction - schema/field-id drift) lives in
# 04371_iceberg_write_schema_drift_guards_compaction_109838.sh.
# Part 4 (OPTIMIZE compaction - spec/leak/evolution) lives in
# 04372_iceberg_write_schema_drift_guards_compaction_spec_109838.sh.
# Part 5 (metadata edge cases) lives in 04373_iceberg_write_schema_drift_guards_metadata_109838.sh.
# The Iceberg write paths map input block columns positionally onto schema fields, so a stale
# attached table could abort the server (field_ids[] out of bounds) or silently commit data
# files with the wrong names/types/field-ids. Each scenario asserts a clean query error, not an
# abort.

# ============================================================================================
# INSERT sink
# ============================================================================================
# These two scenarios run in clickhouse-local, not against the shared test server: on unpatched
# master the column-count-mismatch INSERT aborts the process (getColumnSizes indexes field_ids[]
# out of bounds). Against clickhouse-client that abort lands in the long-running server, the
# hung-check terminates the runner before a FAIL is recorded, and Bugfix validation cannot invert
# it to OK (amd64 tolerates it, aarch64 reports "server died"). clickhouse-local contains the
# abort to a short-lived subprocess: the runner sees a non-zero exit + empty stdout, diffs it
# against .reference, and reports a normal FAIL. The sibling table on the same path is a second
# IcebergLocal attachment (a different table name), matching the server scenario; clickhouse-local
# only rejects a second attachment under the SAME name, so distinct names work here.

# --- INSERT column-count mismatch (sibling DROP COLUMN): rejected, no abort ------------------
INSERT_DIR="${CLICKHOUSE_TMP}/04365_insert_dropcol_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${INSERT_DIR}"; mkdir -p "${INSERT_DIR}/t"
${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0 --multiquery -q "
CREATE TABLE src (c0 Int64, c1 Int64, c2 Int64) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t (c0 Int64, c1 Int64, c2 Int64) ENGINE = IcebergLocal('${INSERT_DIR}/t/', 'Avro');
INSERT INTO t VALUES (0, 0, 0);
CREATE MATERIALIZED VIEW mv TO t AS SELECT c0, c1, c2 FROM src;
CREATE TABLE IF NOT EXISTS tsib (c0 Int64, c1 Int64, c2 Int64) ENGINE = IcebergLocal('${INSERT_DIR}/t/', 'Avro');
ALTER TABLE tsib DROP COLUMN c2;
INSERT INTO src VALUES (1, 2, 3);
" -- --user_files_path="${INSERT_DIR}" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -1
rm -rf "${INSERT_DIR}"

# --- INSERT same-width RENAME drift: rejected by the full (names+types) check -----------------
RENAME_DIR="${CLICKHOUSE_TMP}/04365_insert_rename_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${RENAME_DIR}"; mkdir -p "${RENAME_DIR}/t"
${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0 --multiquery -q "
CREATE TABLE src (c0 Int64, c1 Int64) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t (c0 Int64, c1 Int64) ENGINE = IcebergLocal('${RENAME_DIR}/t/', 'Avro');
INSERT INTO t VALUES (0, 0);
CREATE MATERIALIZED VIEW mv TO t AS SELECT c0, c1 FROM src;
CREATE TABLE IF NOT EXISTS tsib (c0 Int64, c1 Int64) ENGINE = IcebergLocal('${RENAME_DIR}/t/', 'Avro');
ALTER TABLE tsib RENAME COLUMN c1 TO c1_renamed;
INSERT INTO src VALUES (1, 2);
" -- --user_files_path="${RENAME_DIR}" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -1
rm -rf "${RENAME_DIR}"

# --- No drift, but the write mapping collapses the type: accepted -----------------------------
# UInt32/UInt64/Date/DateTime each share an Iceberg primitive with a signed or wider type, so the
# schema check must compare Iceberg-equivalent types. The materialized view is what makes it
# reachable, and no target may receive a direct INSERT first: that refreshes the cached metadata.
LOSSY_DIR="${CLICKHOUSE_TMP}/04365_insert_lossy_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${LOSSY_DIR}"; mkdir -p "${LOSSY_DIR}"/t_u32 "${LOSSY_DIR}"/t_u64 "${LOSSY_DIR}"/t_date "${LOSSY_DIR}"/t_dt
${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0 --multiquery -q "
CREATE TABLE src_u32 (c0 UInt32) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t_u32 (c0 UInt32) ENGINE = IcebergLocal('${LOSSY_DIR}/t_u32/', 'Avro');
CREATE MATERIALIZED VIEW mv_u32 TO t_u32 AS SELECT c0 FROM src_u32;
INSERT INTO src_u32 VALUES (1);
SELECT 'ok_u32', count() FROM t_u32;

CREATE TABLE src_u64 (c0 UInt64) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t_u64 (c0 UInt64) ENGINE = IcebergLocal('${LOSSY_DIR}/t_u64/', 'Avro');
CREATE MATERIALIZED VIEW mv_u64 TO t_u64 AS SELECT c0 FROM src_u64;
INSERT INTO src_u64 VALUES (1);
SELECT 'ok_u64', count() FROM t_u64;

CREATE TABLE src_date (c0 Date) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t_date (c0 Date) ENGINE = IcebergLocal('${LOSSY_DIR}/t_date/', 'Avro');
CREATE MATERIALIZED VIEW mv_date TO t_date AS SELECT c0 FROM src_date;
INSERT INTO src_date VALUES ('2020-01-01');
SELECT 'ok_date', count() FROM t_date;

CREATE TABLE src_dt (c0 DateTime) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t_dt (c0 DateTime) ENGINE = IcebergLocal('${LOSSY_DIR}/t_dt/', 'Avro');
CREATE MATERIALIZED VIEW mv_dt TO t_dt AS SELECT c0 FROM src_dt;
INSERT INTO src_dt VALUES ('2020-01-01 00:00:00');
SELECT 'ok_dt', count() FROM t_dt;
" -- --user_files_path="${LOSSY_DIR}" 2>&1 | grep -oE "^(ok_u32|ok_u64|ok_date|ok_dt)\s1$"
rm -rf "${LOSSY_DIR}"

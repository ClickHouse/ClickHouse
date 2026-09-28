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
# These scenarios run in clickhouse-local, not against the shared test server: the write they
# exercise aborted the process on unpatched master (getColumnSizes indexed field_ids[] out of
# bounds). Against clickhouse-client that abort lands in the long-running server, the hung-check
# terminates the runner before a FAIL is recorded, and Bugfix validation cannot invert it to OK
# (amd64 tolerates it, aarch64 reports "server died"). clickhouse-local contains the abort to a
# short-lived subprocess: the runner sees a non-zero exit + empty stdout, diffs it against
# .reference, and reports a normal FAIL. The sibling table on the same path is a second
# IcebergLocal attachment (a different table name), matching the server scenario; clickhouse-local
# only rejects a second attachment under the SAME name, so distinct names work here.
#
# What the three sibling-ALTER scenarios pin is the absence of that abort, not a rejection: the
# materialized view's target has its Iceberg metadata refreshed before the sink is built, so the
# write is mapped onto the CURRENT schema and the rows read back below are what lands. A MergeTree
# target under the same ALTER stores the same rows, so the values are materialized-view semantics
# rather than an Iceberg-specific narrowing. A sink that still sees a stale input header rejects
# it; that path is exercised in part 5.

# --- INSERT after a sibling DROP COLUMN: mapped onto the narrowed schema, no abort ------------
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
SELECT 'drop_col', c0, c1 FROM t ORDER BY c0;
" -- --user_files_path="${INSERT_DIR}" < /dev/null 2>&1 | grep -oE "^drop_col\s\S.*$"
rm -rf "${INSERT_DIR}"

# --- INSERT after a sibling RENAME COLUMN: the renamed field takes its default ----------------
# The view supplies c1, the refreshed schema wants c1_renamed, so the field is written from its
# default (0) and c1's value is not carried over. Read back, so a corrupted write cannot pass.
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
SELECT 'rename', c0, c1_renamed FROM t ORDER BY c0;
" -- --user_files_path="${RENAME_DIR}" < /dev/null 2>&1 | grep -oE "^rename\s\S.*$"
rm -rf "${RENAME_DIR}"

# --- No drift, but the write mapping collapses the type: accepted -----------------------------
# UInt32/UInt64/Date each share an Iceberg primitive with a signed or wider type, so the schema
# check must compare Iceberg-equivalent types. The materialized view is what makes it reachable,
# and no target may receive a direct INSERT first: that refreshes the cached metadata.
# Values are read back, not counted: a count also passes when the row is written corrupted.
# DateTime/DateTime64(3) are deliberately absent - they map to `timestamp`, whose reader is
# DateTime64(6), and the value survives neither this path nor a plain INSERT on master. That is a
# separate pre-existing write-side scale bug, so asserting on it here would pin corrupted data.
LOSSY_DIR="${CLICKHOUSE_TMP}/04365_insert_lossy_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${LOSSY_DIR}"; mkdir -p "${LOSSY_DIR}"/t_u32 "${LOSSY_DIR}"/t_u64 "${LOSSY_DIR}"/t_date "${LOSSY_DIR}"/t_dt64
${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0 --multiquery -q "
CREATE TABLE src_u32 (c0 UInt32) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t_u32 (c0 UInt32) ENGINE = IcebergLocal('${LOSSY_DIR}/t_u32/', 'Avro');
CREATE MATERIALIZED VIEW mv_u32 TO t_u32 AS SELECT c0 FROM src_u32;
INSERT INTO src_u32 VALUES (7);
SELECT 'ok_u32', toString(c0) FROM t_u32;

CREATE TABLE src_u64 (c0 UInt64) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t_u64 (c0 UInt64) ENGINE = IcebergLocal('${LOSSY_DIR}/t_u64/', 'Avro');
CREATE MATERIALIZED VIEW mv_u64 TO t_u64 AS SELECT c0 FROM src_u64;
INSERT INTO src_u64 VALUES (123456789012);
SELECT 'ok_u64', toString(c0) FROM t_u64;

CREATE TABLE src_date (c0 Date) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t_date (c0 Date) ENGINE = IcebergLocal('${LOSSY_DIR}/t_date/', 'Avro');
CREATE MATERIALIZED VIEW mv_date TO t_date AS SELECT c0 FROM src_date;
INSERT INTO src_date VALUES ('2020-01-02');
SELECT 'ok_date', toString(c0) FROM t_date;

CREATE TABLE src_dt64 (c0 DateTime64(6)) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t_dt64 (c0 DateTime64(6)) ENGINE = IcebergLocal('${LOSSY_DIR}/t_dt64/', 'Avro');
CREATE MATERIALIZED VIEW mv_dt64 TO t_dt64 AS SELECT c0 FROM src_dt64;
INSERT INTO src_dt64 VALUES ('2020-01-02 03:04:05.123456');
SELECT 'ok_dt64', toString(c0) FROM t_dt64;
" -- --user_files_path="${LOSSY_DIR}" < /dev/null 2>&1 | grep -oE "^ok_(u32|u64|date|dt64)\s\S.*$"
rm -rf "${LOSSY_DIR}"

# --- INSERT after a sibling MODIFY COLUMN (int -> long): the value is widened, not truncated --
# Same column name, genuinely different Iceberg primitive: the view's Int32 value must arrive
# under the schema's current long type, so read it back rather than counting rows.
MODIFY_DIR="${CLICKHOUSE_TMP}/04365_insert_modify_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${MODIFY_DIR}"; mkdir -p "${MODIFY_DIR}/t"
${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0 --multiquery -q "
CREATE TABLE src (c0 Int32) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t (c0 Int32) ENGINE = IcebergLocal('${MODIFY_DIR}/t/', 'Avro');
INSERT INTO t VALUES (0);
CREATE MATERIALIZED VIEW mv TO t AS SELECT c0 FROM src;
CREATE TABLE IF NOT EXISTS tsib (c0 Int32) ENGINE = IcebergLocal('${MODIFY_DIR}/t/', 'Avro');
ALTER TABLE tsib MODIFY COLUMN c0 Int64;
INSERT INTO src VALUES (1);
SELECT 'modify', toString(c0) FROM t ORDER BY c0;
" -- --user_files_path="${MODIFY_DIR}" < /dev/null 2>&1 | grep -oE "^modify\s\S.*$"
rm -rf "${MODIFY_DIR}"

#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the native Parquet reader applies the row policy inside the read.

# A row policy on `File(TSV)` stays a `FilterStep` above the reader, so the reader still sees every
# row and would cache the full count even with the `hasFilter` guard removed. `Parquet` supports
# `PREWHERE`, so the policy is `row_level_filter` and the reader emits only the surviving rows.
# That reduced count must not be stored for a later unrestricted `count()`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}/05292_count_from_files_cache_row_policy.parquet"
mkdir -p "${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_05292;
DROP ROW POLICY IF EXISTS p_05292 ON ${CLICKHOUSE_DATABASE}.t_05292;

CREATE TABLE ${CLICKHOUSE_DATABASE}.t_05292 (x UInt8)
ENGINE = File(Parquet, '${CLICKHOUSE_DATABASE}/05292_count_from_files_cache_row_policy.parquet');
INSERT INTO ${CLICKHOUSE_DATABASE}.t_05292 SELECT number FROM numbers(10);

CREATE ROW POLICY p_05292 ON ${CLICKHOUSE_DATABASE}.t_05292 USING x < 5 TO ALL;
"

# `SchemaCache` drops a count when `st_mtime >= registration_time`, and both are second resolution.
# Backdate the file so a reduced count stored by the filtered read stays reusable.
touch -d '2020-01-01 00:00:00' "$DATA_FILE"

$CLICKHOUSE_CLIENT -q "
SELECT 'with_policy', count()
FROM ${CLICKHOUSE_DATABASE}.t_05292
SETTINGS
    use_cache_for_count_from_files = 1,
    optimize_count_from_files = 1,
    input_format_parquet_use_native_reader_v3 = 1,
    max_threads = 1;

DROP ROW POLICY p_05292 ON ${CLICKHOUSE_DATABASE}.t_05292;

SELECT 'after_drop_policy', count()
FROM ${CLICKHOUSE_DATABASE}.t_05292
SETTINGS
    use_cache_for_count_from_files = 1,
    optimize_count_from_files = 1,
    input_format_parquet_use_native_reader_v3 = 1,
    max_threads = 1;

DROP TABLE ${CLICKHOUSE_DATABASE}.t_05292;
"

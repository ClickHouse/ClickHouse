#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so `deltaLakeLocal` is absent.

# Pruning of Delta files by a comparison over `CAST` / `toDate` / `toDate32` of a `Date32` column.
# A `Date` result is fenced: files with values outside the `Date` domain stay scanned, because
# the conversion wraps, saturates or throws there. Each query stores its rows in `results`, and the
# final report prints them next to the number of scanned files or the error from `system.query_log`.
# A query repeated with `delta_lake_enable_engine_predicate = 0` must return the same rows; the
# report prints every pair that does not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_cast"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

# Data files. Most hold one row and are named after it.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${ROOT}/t/{_partition_id}.parquet', Parquet, 'd Date32') PARTITION BY toString(d) VALUES ('2026-01-01'), ('2026-01-02'), ('2205-06-08');
    INSERT INTO FUNCTION file('${ROOT}/lower/{_partition_id}.parquet', Parquet, 'd Date32') PARTITION BY toString(d) VALUES ('1969-12-31'), ('1970-01-01'), ('1970-01-02');
    INSERT INTO FUNCTION file('${ROOT}/upper/{_partition_id}.parquet', Parquet, 'd Date32') PARTITION BY toString(d) VALUES ('2149-06-05'), ('2149-06-06'), ('2149-06-07');
    INSERT INTO FUNCTION file('${ROOT}/negation/{_partition_id}.parquet', Parquet, 'd Date32') PARTITION BY toString(d) VALUES ('2026-01-01'), ('2026-01-02'), ('2205-06-09');
    INSERT INTO FUNCTION file('${ROOT}/aliases/{_partition_id}.parquet', Parquet, 'd Date32') PARTITION BY toString(d) VALUES ('1846-07-28'), ('2026-01-02'), ('2384-11-12');
    INSERT INTO FUNCTION file('${ROOT}/valid/{_partition_id}.parquet', Parquet, 'd Date32') PARTITION BY toString(d) VALUES ('1970-01-02'), ('2026-01-01');
    INSERT INTO FUNCTION file('${ROOT}/no_stats/{_partition_id}.parquet', Parquet, 'd Date32') PARTITION BY toString(d) VALUES ('2026-01-01'), ('2026-01-02');
    INSERT INTO FUNCTION file('${ROOT}/no_min/{_partition_id}.parquet', Parquet, 'd Date32') PARTITION BY toString(d) VALUES ('2026-01-01'), ('2026-01-02');
    INSERT INTO FUNCTION file('${ROOT}/no_max/{_partition_id}.parquet', Parquet, 'd Date32') PARTITION BY toString(d) VALUES ('2026-01-01'), ('2026-01-02');
    INSERT INTO FUNCTION file('${ROOT}/mixed/{_partition_id}.parquet', Parquet, 'd Date32') PARTITION BY toString(d) VALUES ('2026-01-01'), ('2026-01-02');
    INSERT INTO FUNCTION file('${ROOT}/mixed/low.parquet', Parquet, 'd Date32') VALUES ('1969-12-31'), ('1970-01-01');
    INSERT INTO FUNCTION file('${ROOT}/mixed/high.parquet', Parquet, 'd Date32') VALUES ('2149-06-06'), ('2149-06-07');
    INSERT INTO FUNCTION file('${ROOT}/spanning/{_partition_id}.parquet', Parquet, 'd Date32') PARTITION BY toString(d) VALUES ('2026-01-01');
    INSERT INTO FUNCTION file('${ROOT}/spanning/wide.parquet', Parquet, 'd Date32') VALUES ('1970-01-01'), ('1970-01-02'), ('2149-06-07');
    INSERT INTO FUNCTION file('${ROOT}/nullable/{_partition_id}.parquet', Parquet, 'd Nullable(Date32)') PARTITION BY toString(assumeNotNull(d)) VALUES ('2026-01-01'), ('2026-01-02'), ('2205-06-08');
    INSERT INTO FUNCTION file('${ROOT}/nullable/null.parquet', Parquet, 'd Nullable(Date32)') VALUES (NULL);
    INSERT INTO FUNCTION file('${ROOT}/nullable_with_nulls/{_partition_id}.parquet', Parquet, 'd Nullable(Date32)') PARTITION BY toString(assumeNotNull(d)) VALUES ('2026-01-01'), ('2026-01-02');
    INSERT INTO FUNCTION file('${ROOT}/nullable_with_nulls/with_null.parquet', Parquet, 'd Nullable(Date32)') VALUES ('2026-01-03'), (NULL);
    INSERT INTO FUNCTION file('${ROOT}/nullable_no_null_count/{_partition_id}.parquet', Parquet, 'd Nullable(Date32)') PARTITION BY toString(assumeNotNull(d)) VALUES ('2026-01-01'), ('2026-01-02');
    INSERT INTO FUNCTION file('${ROOT}/overflow_rhs/{_partition_id}.parquet', Parquet, 'd Nullable(Date32)') PARTITION BY toString(assumeNotNull(d)) VALUES ('1970-01-02'), ('2026-01-01');
    INSERT INTO FUNCTION file('${ROOT}/partitioned/2026-01-01.parquet', Parquet, 'x Int32') VALUES (1);
    INSERT INTO FUNCTION file('${ROOT}/partitioned/2026-01-02.parquet', Parquet, 'x Int32') VALUES (1);
    INSERT INTO FUNCTION file('${ROOT}/partitioned/2205-06-08.parquet', Parquet, 'x Int32') VALUES (1);
    INSERT INTO FUNCTION file('${ROOT}/partitioned/null.parquet', Parquet, 'x Int32') VALUES (1);
" < /dev/null

# Hand-written Delta logs, because ClickHouse's Delta writer emits no per-column stats.
PROTOCOL='{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}'
DATE32='{"metaData":{"id":"date32","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"d\",\"type\":\"date\",\"nullable\":false,\"metadata\":{}}]}","partitionColumns":[],"configuration":{}}}'
NULLABLE='{"metaData":{"id":"nullable","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"d\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{}}}'
PARTITIONED='{"metaData":{"id":"partitioned","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"x\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"d\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["d"],"configuration":{}}}'

# log <table> <metaData> <add>...: writes the table's first commit.
log() {
    mkdir -p "${ROOT}/$1/_delta_log"
    printf '%s\n' "${PROTOCOL}" "$2" "${@:3}" > "${ROOT}/$1/_delta_log/00000000000000000000.json"
}

# add <file> <stats>: an add action; `stats` is a JSON document embedded as a JSON string.
add() {
    printf '{"add":{"path":"%s.parquet","partitionValues":{},"size":1,"modificationTime":0,"dataChange":true,"stats":"%s"}}' "$1" "${2//\"/\\\"}"
}

# one <date>: the single-row file `<date>.parquet` with full stats.
one() {
    add "$1" "{\"numRecords\":1,\"minValues\":{\"d\":\"$1\"},\"maxValues\":{\"d\":\"$1\"},\"nullCount\":{\"d\":0}}"
}

# partition <file> <value>: the file of partition `d = <value>`, a JSON string or `null`.
partition() {
    printf '{"add":{"path":"%s.parquet","partitionValues":{"d":%s},"size":1,"modificationTime":0,"dataChange":true,"stats":"{\\"numRecords\\":1}"}}' "$1" "$2"
}

log t "${DATE32}" "$(one 2026-01-01)" "$(one 2026-01-02)" "$(one 2205-06-08)"
log lower "${DATE32}" "$(one 1969-12-31)" "$(one 1970-01-01)" "$(one 1970-01-02)"
log upper "${DATE32}" "$(one 2149-06-05)" "$(one 2149-06-06)" "$(one 2149-06-07)"
log negation "${DATE32}" "$(one 2026-01-01)" "$(one 2026-01-02)" "$(one 2205-06-09)"
log aliases "${DATE32}" "$(one 1846-07-28)" "$(one 2026-01-02)" "$(one 2384-11-12)"
log valid "${DATE32}" "$(one 1970-01-02)" "$(one 2026-01-01)"
log no_stats "${DATE32}" \
    "$(add 2026-01-01 '{"numRecords":1}')" \
    "$(add 2026-01-02 '{"numRecords":1}')"
log no_min "${DATE32}" \
    "$(add 2026-01-01 '{"numRecords":1,"maxValues":{"d":"2026-01-01"},"nullCount":{"d":0}}')" \
    "$(add 2026-01-02 '{"numRecords":1,"maxValues":{"d":"2026-01-02"},"nullCount":{"d":0}}')"
log no_max "${DATE32}" \
    "$(add 2026-01-01 '{"numRecords":1,"minValues":{"d":"2026-01-01"},"nullCount":{"d":0}}')" \
    "$(add 2026-01-02 '{"numRecords":1,"minValues":{"d":"2026-01-02"},"nullCount":{"d":0}}')"
log mixed "${DATE32}" "$(one 2026-01-01)" "$(one 2026-01-02)" \
    "$(add low '{"numRecords":2,"minValues":{"d":"1969-12-31"},"maxValues":{"d":"1970-01-01"},"nullCount":{"d":0}}')" \
    "$(add high '{"numRecords":2,"minValues":{"d":"2149-06-06"},"maxValues":{"d":"2149-06-07"},"nullCount":{"d":0}}')"
log spanning "${DATE32}" "$(one 2026-01-01)" \
    "$(add wide '{"numRecords":3,"minValues":{"d":"1970-01-01"},"maxValues":{"d":"2149-06-07"},"nullCount":{"d":0}}')"
log nullable "${NULLABLE}" "$(one 2026-01-01)" "$(one 2026-01-02)" "$(one 2205-06-08)" \
    "$(add null '{"numRecords":1,"nullCount":{"d":1}}')"
log nullable_with_nulls "${NULLABLE}" "$(one 2026-01-01)" "$(one 2026-01-02)" \
    "$(add with_null '{"numRecords":2,"minValues":{"d":"2026-01-03"},"maxValues":{"d":"2026-01-03"},"nullCount":{"d":1}}')"
log nullable_no_null_count "${NULLABLE}" \
    "$(add 2026-01-01 '{"numRecords":1,"minValues":{"d":"2026-01-01"},"maxValues":{"d":"2026-01-01"}}')" \
    "$(add 2026-01-02 '{"numRecords":1,"minValues":{"d":"2026-01-02"},"maxValues":{"d":"2026-01-02"}}')"
log overflow_rhs "${NULLABLE}" "$(one 1970-01-02)" "$(one 2026-01-01)"
log partitioned "${PARTITIONED}" \
    "$(partition 2026-01-01 '"2026-01-01"')" \
    "$(partition 2026-01-02 '"2026-01-02"')" \
    "$(partition 2205-06-08 '"2205-06-08"')" \
    "$(partition null null)"

Q="INSERT INTO results SELECT queryID(), arraySort(groupArray(toString(d))) FROM deltaLakeLocal"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE results (query_id String, rows Array(String)) ENGINE = Memory;

    -- Only the engine predicate may skip files; NULL and overflow errors must not depend on evaluation order.
    SET allow_delta_kernel_rs = 1, delta_lake_throw_on_engine_predicate_error = 1,
        input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0,
        input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0,
        optimize_move_to_prewhere = 0, convert_query_to_cnf = 0, short_circuit_function_evaluation = 'disable',
        date_time_overflow_behavior = 'ignore', cast_keep_nullable = 0, session_timezone = 'UTC',
        use_structure_from_insertion_table_in_table_functions = 0, log_queries = 1;

    -- A plain column, then the conversions. 2205-06-08 wraps to the \`Date\` 2026-01-01, so its file is kept for \`Date\`.
    ${Q}('${ROOT}/t') WHERE d = '2026-01-01';
    ${Q}('${ROOT}/t') WHERE CAST(d AS Date) = '2026-01-01';
    ${Q}('${ROOT}/t') WHERE toDate(d) = '2026-01-01';
    ${Q}('${ROOT}/t') WHERE toDate(d, 'UTC') = '2026-01-01';
    ${Q}('${ROOT}/t') WHERE d::Date32 = '2026-01-01';
    ${Q}('${ROOT}/t') WHERE d::Date != '2026-01-01';
    ${Q}('${ROOT}/t') WHERE toDate(d) = '2026-01-03';
    ${Q}('${ROOT}/t') WHERE toDate32(d) = '2026-01-03';
    ${Q}('${ROOT}/t') WHERE toDate32(d) < '2026-01-02';
    ${Q}('${ROOT}/t') WHERE '2026-01-02' > toDate32(d);
    ${Q}('${ROOT}/t') WHERE d::Date32 <= '2026-01-01';
    ${Q}('${ROOT}/t') WHERE toDate(d) < '2026-01-02';
    ${Q}('${ROOT}/t') WHERE toDate(d) >= '2026-01-02';
    ${Q}('${ROOT}/t') WHERE NOT (toDate(d) < '2026-01-02');
    ${Q}('${ROOT}/t') WHERE toDate32(d) = toDate('2026-01-01');
    ${Q}('${ROOT}/t') WHERE toDate(d) = toDate32('2026-01-01');
    ${Q}('${ROOT}/t') WHERE toDate(d) = toDate32('2205-06-08');
    ${Q}('${ROOT}/negation') WHERE NOT (toDate(d) = '2026-01-01' AND d > toDate32('1970-01-01'));
    ${Q}('${ROOT}/aliases') WHERE toDate(d) = '2026-01-01';
    ${Q}('${ROOT}/valid') WHERE toDate(d) != toDateTime('2026-01-01 00:00:01', 'UTC');
    ${Q}('${ROOT}/valid') WHERE CAST(d AS Date) = '2026-01-01' SETTINGS date_time_overflow_behavior = 'throw';

    -- Values next to the \`Date\` bounds.
    ${Q}('${ROOT}/lower') WHERE toDate(d) = '1970-01-01' SETTINGS date_time_overflow_behavior = 'saturate';
    ${Q}('${ROOT}/upper') WHERE CAST(d AS Date) = '2149-06-06' SETTINGS date_time_overflow_behavior = 'saturate';
    ${Q}('${ROOT}/upper') WHERE toDate(d) > '2149-06-05' SETTINGS date_time_overflow_behavior = 'saturate';
    ${Q}('${ROOT}/mixed') WHERE toDate(d) = '1970-01-01';
    ${Q}('${ROOT}/mixed') WHERE toDate(d) = '2149-06-06';
    ${Q}('${ROOT}/spanning') WHERE toDate(d) = '1970-01-02';

    -- An overflow must still throw with the engine predicate, also when only the literal overflows.
    ${Q}('${ROOT}/lower') WHERE CAST(d AS Date) = '2026-01-01' SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
    ${Q}('${ROOT}/lower') WHERE NOT (toDate(d) = '2026-01-01' AND d > toDate32('1900-01-01')) SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
    ${Q}('${ROOT}/upper') WHERE CAST(d AS Date) = '2026-01-01' SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
    ${Q}('${ROOT}/upper') WHERE NOT (toDate(d) = '2026-01-01' AND d > toDate32('1900-01-01')) SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
    ${Q}('${ROOT}/upper') WHERE toDate(d) < '2026-01-01' SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
    ${Q}('${ROOT}/overflow_rhs') WHERE toDate(d) = '1969-12-31' SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

    -- Missing stats: without both bounds a \`Date\` result cannot be proven in range.
    ${Q}('${ROOT}/no_stats') WHERE toDate(d) = '2026-01-01';
    ${Q}('${ROOT}/no_min') WHERE toDate(d) = '2026-01-01';
    ${Q}('${ROOT}/no_min') WHERE toDate32(d) = '2026-01-02';
    ${Q}('${ROOT}/no_max') WHERE toDate(d) = '2026-01-01';
    ${Q}('${ROOT}/no_max') WHERE toDate32(d) = '2026-01-01';

    -- Nullable columns. A non-nullable cast throws on a NULL row, so a full scan fails,
    -- and the query succeeds only when min/max prune the file with the NULL.
    ${Q}('${ROOT}/nullable') WHERE toDate(d) = '2026-01-01';
    ${Q}('${ROOT}/nullable') WHERE CAST(d AS Nullable(Date)) = toNullable(toDate('2026-01-01'));
    ${Q}('${ROOT}/nullable') WHERE CAST(d AS Date) = '2026-01-01' SETTINGS delta_lake_enable_engine_predicate = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
    ${Q}('${ROOT}/nullable') WHERE CAST(d AS Date) = '2026-01-01';
    ${Q}('${ROOT}/nullable') WHERE NOT (CAST(d AS Date) = '2026-01-01');
    ${Q}('${ROOT}/nullable_with_nulls') WHERE d::Date = '2026-01-01' SETTINGS delta_lake_enable_engine_predicate = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
    ${Q}('${ROOT}/nullable_with_nulls') WHERE d::Date = '2026-01-01';
    ${Q}('${ROOT}/nullable_with_nulls') WHERE d::Date32 = '2026-01-01';
    ${Q}('${ROOT}/nullable_with_nulls') WHERE NOT (d::Date = '2026-01-03');
    ${Q}('${ROOT}/nullable_with_nulls') WHERE d::Date = '2026-01-03'; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
    ${Q}('${ROOT}/nullable_no_null_count') WHERE d::Date = '2026-01-01';
    ${Q}('${ROOT}/partitioned') WHERE d::Date = '2026-01-01' SETTINGS delta_lake_enable_engine_predicate = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
    ${Q}('${ROOT}/partitioned') WHERE d::Date = '2026-01-01';
    ${Q}('${ROOT}/partitioned') WHERE d::Date32 = '2026-01-02';
    ${Q}('${ROOT}/partitioned') WHERE d::Date != '2026-01-01';
    ${Q}('${ROOT}/partitioned') WHERE NOT (d::Date = '2026-01-01');

    -- Full scans of the cases where wrong pruning would lose rows.
    ${Q}('${ROOT}/t') WHERE CAST(d AS Date) = '2026-01-01' SETTINGS delta_lake_enable_engine_predicate = 0;
    ${Q}('${ROOT}/t') WHERE d::Date != '2026-01-01' SETTINGS delta_lake_enable_engine_predicate = 0;
    ${Q}('${ROOT}/t') WHERE toDate32(d) < '2026-01-02' SETTINGS delta_lake_enable_engine_predicate = 0;
    ${Q}('${ROOT}/negation') WHERE NOT (toDate(d) = '2026-01-01' AND d > toDate32('1970-01-01')) SETTINGS delta_lake_enable_engine_predicate = 0;
    ${Q}('${ROOT}/aliases') WHERE toDate(d) = '2026-01-01' SETTINGS delta_lake_enable_engine_predicate = 0;
    ${Q}('${ROOT}/lower') WHERE toDate(d) = '1970-01-01' SETTINGS date_time_overflow_behavior = 'saturate', delta_lake_enable_engine_predicate = 0;
    ${Q}('${ROOT}/upper') WHERE toDate(d) > '2149-06-05' SETTINGS date_time_overflow_behavior = 'saturate', delta_lake_enable_engine_predicate = 0;
    ${Q}('${ROOT}/mixed') WHERE toDate(d) = '1970-01-01' SETTINGS delta_lake_enable_engine_predicate = 0;
    ${Q}('${ROOT}/mixed') WHERE toDate(d) = '2149-06-06' SETTINGS delta_lake_enable_engine_predicate = 0;
    ${Q}('${ROOT}/spanning') WHERE toDate(d) = '1970-01-02' SETTINGS delta_lake_enable_engine_predicate = 0;
    ${Q}('${ROOT}/no_min') WHERE toDate32(d) = '2026-01-02' SETTINGS delta_lake_enable_engine_predicate = 0;

    SYSTEM FLUSH LOGS query_log;
    CREATE VIEW report AS SELECT
        event_time_microseconds,
        extract(query, '_delta_cast/([a-z_0-9]+)') AS table,
        extract(query, ' WHERE ([^;]*)') AS condition,
        rows,
        exception_code,
        -- Scan counts are not pinned after an error.
        if(exception_code = 0, toString(ProfileEvents['DeltaLakeScannedFiles']), errorCodeToName(exception_code)) AS outcome
    FROM system.query_log LEFT JOIN results USING (query_id)
    WHERE current_database = currentDatabase() AND event_date >= yesterday()
        AND query_kind = 'Insert' AND type != 'QueryStart' AND query LIKE '%INSERT INTO results %';

    SELECT table, condition, rows, outcome FROM report ORDER BY event_time_microseconds FORMAT TSVRaw;

    SELECT 'engine predicate changed rows', table, key, groupUniqArray(rows)
    FROM (SELECT table, rows, replaceRegexpOne(condition, '(,| SETTINGS) delta_lake_enable_engine_predicate = 0', '') AS key
          FROM report WHERE exception_code = 0)
    GROUP BY table, key
    HAVING length(groupUniqArray(rows)) > 1
    FORMAT TSVRaw;
" < /dev/null

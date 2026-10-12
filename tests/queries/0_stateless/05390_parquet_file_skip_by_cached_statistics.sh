#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# A read of local Parquet files keeps the min/max statistics ranges of its filter columns with the cached file
# metadata. A later read of the same file version, also with other constants, skips the files whose row groups
# cannot match without opening them (`ParquetFilesSkippedByCachedStatistics`). The query condition cache is off
# in most queries, so that it cannot be the reason for skipping. Files are read as usual if their statistics
# were kept for another column type, if their version token has not settled, or after a rewrite.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_DATABASE}_05390"
DATA_DIR="${USER_FILES_PATH:?}/${DIR}"
rm -rf "${DATA_DIR:?}"
mkdir -p "$DATA_DIR"

# File i holds x in [i * 1000, i * 1000 + 999] in two row groups.
write_file() # name, query
{
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${DIR}/$1.parquet', Parquet, 'x UInt32, s String') $2
        SETTINGS output_format_parquet_row_group_size = 500, engine_file_truncate_on_insert = 1"
}
for i in 0 1 2 3; do
    write_file "f$i" "SELECT $i * 1000 + number, toString(number) FROM numbers(1000)"
done
# Settled version tokens: statistics are kept and used only for files modified a while ago.
touch -d '2020-01-01 00:00:00' "$DATA_DIR"/f*.parquet

echo "row groups per file:"
${CLICKHOUSE_CLIENT} --query "SELECT num_row_groups FROM file('${DIR}/f0.parquet', ParquetMetadata)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05390"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05390_i64"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05390 (x UInt32, s String) ENGINE = File(Parquet, '${DIR}/f*.parquet')"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05390_i64 (x Int64, s String) ENGINE = File(Parquet, '${DIR}/f*.parquet')"

PINNED="input_format_parquet_filter_push_down = 1, use_parquet_metadata_cache = 1"
n=0
run() # label, query, [settings], [print pruned row groups]
{
    n=$((n + 1))
    local qid="${CLICKHOUSE_TEST_UNIQUE_NAME}_$n"
    local result
    result=$(${CLICKHOUSE_CLIENT} --query_id="$qid" --query "$2 SETTINGS ${3:-use_query_condition_cache = 0, $PINNED}" | tr '\t' ' ')
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    local events
    events=$(${CLICKHOUSE_CLIENT} --query "
        SELECT 'skipped ' || toString(ProfileEvents['ParquetFilesSkippedByCachedStatistics'])
            || if('$4' = '', '', ', pruned row groups ' || toString(ProfileEvents['ParquetPrunedRowGroups']))
        FROM system.query_log
        WHERE query_id = '$qid' AND current_database = currentDatabase() AND type = 'QueryFinish'")
    echo "$1: $result, $events"
}

Q_RANGE="SELECT count(), sum(x) FROM t_05390 WHERE x BETWEEN 1500 AND 1600"

# The first read keeps the ranges of x; the second skips the three files that cannot match and counts their row
# groups as pruned, like the first read did.
run "range, first read" "$Q_RANGE" "" pruned
run "range, second read" "$Q_RANGE" "" pruned
# Nothing is skipped without the metadata cache.
run "range, metadata cache off" "$Q_RANGE" "use_query_condition_cache = 0, input_format_parquet_filter_push_down = 1, use_parquet_metadata_cache = 0"
# Other constants on the same column use the same ranges.
run "point in f3" "SELECT count(), sum(x) FROM t_05390 WHERE x = 3999"
run "no file matches" "SELECT count(), sum(x) FROM t_05390 WHERE x > 100000"
run "IN list" "SELECT count(), sum(x) FROM t_05390 WHERE x IN (5, 2500, 2501)"
run "OR of ranges" "SELECT count(), sum(x) FROM t_05390 WHERE x < 10 OR x >= 3995"
# A column without kept ranges is unknown: the kept ranges of x skip three files, and the read of f1 keeps the ranges
# of s.
run "x and s" "SELECT count(), sum(x) FROM t_05390 WHERE x BETWEEN 1500 AND 1600 AND s = '501'"
run "s beyond its range, first read" "SELECT count() FROM t_05390 WHERE s = 'zzz'"
run "s beyond its range, second read" "SELECT count() FROM t_05390 WHERE s = 'zzz'"
# A condition that the statistics cannot decide skips nothing.
run "s only" "SELECT count() FROM t_05390 WHERE s = '7'"
# Ranges are kept per column type, and not by a read without the metadata cache.
Q_RANGE_I64="SELECT count(), sum(x) FROM t_05390_i64 WHERE x BETWEEN 1500 AND 1600"
run "Int64 table, metadata cache off" "$Q_RANGE_I64" "use_query_condition_cache = 0, input_format_parquet_filter_push_down = 1, use_parquet_metadata_cache = 0"
run "Int64 table, first read" "$Q_RANGE_I64"
run "Int64 table, second read" "$Q_RANGE_I64"
# The file() table function uses the same ranges. Unlike a File table, which takes its file list and format
# settings from CREATE TABLE, it expands the glob and applies the query settings in every query.
Q_RANGE_FN="SELECT count(), sum(x) FROM file('${DIR}/f*.parquet', Parquet, 'x UInt32, s String') WHERE x BETWEEN 1500 AND 1600"
run "file()" "$Q_RANGE_FN"
# Filter push-down off: no skipping, as no row group pruning.
run "file(), filter push-down off" "$Q_RANGE_FN" "use_query_condition_cache = 0, input_format_parquet_filter_push_down = 0, use_parquet_metadata_cache = 1"
# With the query condition cache on as well.
run "with query condition cache" "SELECT count(), sum(x) FROM t_05390 WHERE x BETWEEN 1700 AND 1800" "use_query_condition_cache = 1, $PINNED"

# An in-place rewrite with rows in the range gives the file a new version token: it is read, not skipped.
write_file f0 "SELECT 1550, 'new' FROM numbers(10)"
touch -d '2021-01-01 00:00:00' "$DATA_DIR/f0.parquet"
run "after rewrite of f0" "$Q_RANGE"
run "after rewrite of f0, again" "$Q_RANGE"

# A file whose version token has not settled (here: a modification time in the future) is neither skipped nor
# are its ranges kept. f4 is not in the file list of t_05390, so file() reads it.
write_file f4 "SELECT 5000 + number, toString(number) FROM numbers(1000)"
touch -d '2099-01-01 00:00:00' "$DATA_DIR/f4.parquet"
run "f4 unsettled" "$Q_RANGE_FN"
run "f4 unsettled, again" "$Q_RANGE_FN"
touch -d '2022-01-01 00:00:00' "$DATA_DIR/f4.parquet"
run "f4 settled, first read" "$Q_RANGE_FN"
run "f4 settled, second read" "$Q_RANGE_FN"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05390"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05390_i64"
rm -rf "${DATA_DIR:?}"

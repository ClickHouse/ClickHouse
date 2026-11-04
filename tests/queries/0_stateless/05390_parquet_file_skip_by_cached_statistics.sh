#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# A read of local Parquet files whose metadata is already cached keeps the min/max statistics ranges of its filter
# columns with it. A later read of the same file version, also with other constants, skips the files whose row groups
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
    echo "INSERT INTO FUNCTION file('${DIR}/$1.parquet', Parquet, 'x UInt32, s String') $2
        SETTINGS output_format_parquet_row_group_size = 500, engine_file_truncate_on_insert = 1;"
}
${CLICKHOUSE_CLIENT} --query "
    $(for i in 0 1 2 3; do write_file "f$i" "SELECT $i * 1000 + number, toString(number) FROM numbers(1000)"; done)
    DROP TABLE IF EXISTS t_05390;
    DROP TABLE IF EXISTS t_05390_i64;
    CREATE TABLE t_05390 (x UInt32, s String) ENGINE = File(Parquet, '${DIR}/f*.parquet');
    CREATE TABLE t_05390_i64 (x Int64, s String) ENGINE = File(Parquet, '${DIR}/f*.parquet');"
# Settled version tokens: statistics are kept and used only for files modified a while ago.
touch -d '2020-01-01 00:00:00' "$DATA_DIR"/f*.parquet

# The queries run in batches, one client call per batch, in their order. Their profile events are read from the query
# log at the end.
PINNED="input_format_parquet_filter_push_down = 1, use_parquet_metadata_cache = 1"
LABELS=()
SHOW_PRUNED=()
RESULTS=()
BATCH=""
add() # label, query, [settings], [print pruned row groups]
{
    LABELS+=("$1")
    SHOW_PRUNED+=("${4:-}")
    BATCH+="$2 SETTINGS ${3:-use_query_condition_cache = 0, $PINNED}, log_comment = '05390 query ${#LABELS[@]}';"
}
run_batch() # [statements to run after the batch]
{
    local line
    while IFS= read -r line; do
        RESULTS+=("${line//$'\t'/ }")
    done < <(${CLICKHOUSE_CLIENT} --query "$BATCH ${1:-}")
    BATCH=""
}

Q_RANGE="SELECT count(), sum(x) FROM t_05390 WHERE x BETWEEN 1500 AND 1600"

add "row groups per file" "SELECT num_row_groups FROM file('${DIR}/f0.parquet', ParquetMetadata)"
# The first read caches the metadata of the files and the second keeps the ranges of x with it. The third skips the
# three files that cannot match and counts their row groups as pruned, like the reads before.
add "range, first read" "$Q_RANGE" "" pruned
add "range, second read" "$Q_RANGE" "" pruned
add "range, third read" "$Q_RANGE" "" pruned
# Nothing is skipped without the metadata cache.
add "range, metadata cache off" "$Q_RANGE" "use_query_condition_cache = 0, input_format_parquet_filter_push_down = 1, use_parquet_metadata_cache = 0"
# Other constants on the same column use the same ranges.
add "point in f3" "SELECT count(), sum(x) FROM t_05390 WHERE x = 3999"
add "no file matches" "SELECT count(), sum(x) FROM t_05390 WHERE x > 100000"
add "IN list" "SELECT count(), sum(x) FROM t_05390 WHERE x IN (5, 2500, 2501)"
add "OR of ranges" "SELECT count(), sum(x) FROM t_05390 WHERE x < 10 OR x >= 3995"
# A column without kept ranges is unknown: the kept ranges of x skip three files, and the read of f1 keeps the ranges
# of s.
add "x and s" "SELECT count(), sum(x) FROM t_05390 WHERE x BETWEEN 1500 AND 1600 AND s = '501'"
add "s beyond its range, first read" "SELECT count() FROM t_05390 WHERE s = 'zzz'"
add "s beyond its range, second read" "SELECT count() FROM t_05390 WHERE s = 'zzz'"
# A condition that the statistics cannot decide skips nothing.
add "s only" "SELECT count() FROM t_05390 WHERE s = '7'"
# Ranges are kept per column type, and not by a read without the metadata cache.
Q_RANGE_I64="SELECT count(), sum(x) FROM t_05390_i64 WHERE x BETWEEN 1500 AND 1600"
add "Int64 table, metadata cache off" "$Q_RANGE_I64" "use_query_condition_cache = 0, input_format_parquet_filter_push_down = 1, use_parquet_metadata_cache = 0"
add "Int64 table, first read" "$Q_RANGE_I64"
add "Int64 table, second read" "$Q_RANGE_I64"
# The file() table function uses the same ranges. Unlike a File table, which takes its file list and format
# settings from CREATE TABLE, it expands the glob and applies the query settings in every query.
Q_RANGE_FN="SELECT count(), sum(x) FROM file('${DIR}/f*.parquet', Parquet, 'x UInt32, s String') WHERE x BETWEEN 1500 AND 1600"
add "file()" "$Q_RANGE_FN"
# Filter push-down off: no skipping, as no row group pruning.
add "file(), filter push-down off" "$Q_RANGE_FN" "use_query_condition_cache = 0, input_format_parquet_filter_push_down = 0, use_parquet_metadata_cache = 1"
# With the query condition cache on as well.
add "with query condition cache" "SELECT count(), sum(x) FROM t_05390 WHERE x BETWEEN 1700 AND 1800" "use_query_condition_cache = 1, $PINNED"
# An in-place rewrite with rows in the range gives the file a new version token: it is read, not skipped.
run_batch "$(write_file f0 "SELECT 1550, 'new' FROM numbers(10)")"
touch -d '2021-01-01 00:00:00' "$DATA_DIR/f0.parquet"

add "after rewrite of f0" "$Q_RANGE"
add "after rewrite of f0, again" "$Q_RANGE"
# A file whose version token has not settled (here: a modification time in the future) is neither skipped nor
# are its ranges kept. f4 is not in the file list of t_05390, so file() reads it.
run_batch "$(write_file f4 "SELECT 5000 + number, toString(number) FROM numbers(1000)")"
touch -d '2099-01-01 00:00:00' "$DATA_DIR/f4.parquet"

add "f4 unsettled" "$Q_RANGE_FN"
add "f4 unsettled, again" "$Q_RANGE_FN"
run_batch
touch -d '2022-01-01 00:00:00' "$DATA_DIR/f4.parquet"

add "f4 settled, first read" "$Q_RANGE_FN"
add "f4 settled, second read" "$Q_RANGE_FN"
add "f4 settled, third read" "$Q_RANGE_FN"
run_batch

# Ranges too large to keep (250 row groups with 3000-byte bounds of four columns) are collected only up to what could
# be kept, for all columns together. The third read of the file, which collects them, takes about as much memory as the
# third read of an unsettled copy.
WIDE_STRUCTURE="x UInt32, s1 String, s2 String, s3 String, s4 String"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DIR}/wide.parquet', Parquet, '$WIDE_STRUCTURE')
    SELECT number, repeat('a', 3000), repeat('b', 3000), repeat('c', 3000), repeat('d', 3000) FROM numbers(250)
    SETTINGS output_format_parquet_row_group_size = 1"
cp "$DATA_DIR/wide.parquet" "$DATA_DIR/wide_unsettled.parquet"
touch -d '2020-01-01 00:00:00' "$DATA_DIR/wide.parquet"
touch -d '2099-01-01 00:00:00' "$DATA_DIR/wide_unsettled.parquet"
Q_WIDE=""
for f in wide wide_unsettled; do
    for n in 1 2 3; do
        Q_WIDE+="SELECT count() FROM file('${DIR}/$f.parquet', Parquet, '$WIDE_STRUCTURE')
            WHERE s1 = 'zzzz' AND s2 = 'zzzz' AND s3 = 'zzzz' AND s4 = 'zzzz'
            SETTINGS use_query_condition_cache = 0, max_untracked_memory = 0, $PINNED, log_comment = '05390 $f $n' FORMAT Null;"
    done
done
${CLICKHOUSE_CLIENT} --query "$Q_WIDE"

SKIPPED=()
PRUNED=()
while read -r i skipped pruned; do
    SKIPPED[i]=$skipped
    PRUNED[i]=$pruned
done < <(${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT toUInt32(substring(log_comment, 13)),
        ProfileEvents['ParquetFilesSkippedByCachedStatistics'], ProfileEvents['ParquetPrunedRowGroups']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
        AND startsWith(log_comment, '05390 query ')")
for ((i = 1; i <= ${#LABELS[@]}; ++i)); do
    line="${LABELS[i - 1]}: ${RESULTS[i - 1]}, skipped ${SKIPPED[i]}"
    [ -n "${SHOW_PRUNED[i - 1]}" ] && line+=", pruned row groups ${PRUNED[i]}"
    echo "$line"
done
echo "ranges too large to keep, extra memory of the read that collects them below 3 MB: $(${CLICKHOUSE_CLIENT} --query "
    SELECT sumIf(memory_usage, log_comment = '05390 wide 3') - sumIf(memory_usage, log_comment = '05390 wide_unsettled 3') < 3000000 AND count() = 2
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
        AND log_comment IN ('05390 wide 3', '05390 wide_unsettled 3')")"

# The kept ranges count in the cache weight as soon as they are kept, and a skip leaves the weight as it is. Reads
# through the wrapper of `aggregate_function_input_format = 'value'` keep the ranges for later reads too. A read of
# a file whose version token has not settled keeps nothing, even when its metadata is cached. The metadata cache of
# a clickhouse-local process serves no other query. Two reads without a filter first put the column chunks of x and s
# in the cell, so that a later filtered read changes its weight only by the ranges it keeps.
touch -d '2099-01-01 00:00:00' "$DATA_DIR/f4.parquet"
Q_WEIGHT="SELECT value FROM system.metrics WHERE metric = 'ParquetMetadataCacheBytes';"
read_with_weight() # file
{
    echo "SELECT count() FROM file('${DATA_DIR}/$1.parquet', Parquet, 'x UInt32, s String') WHERE x > 100000
        SETTINGS use_query_condition_cache = 0, $PINNED FORMAT Null; $Q_WEIGHT"
}
read_columns_twice() # file
{
    local query="SELECT sum(x), max(s) FROM file('${DATA_DIR}/$1.parquet', Parquet, 'x UInt32, s String')
        SETTINGS use_query_condition_cache = 0, $PINNED FORMAT Null;"
    echo "$query $query $Q_WEIGHT"
}
Q_LOCAL=$(read_with_weight f1)
Q_UNSETTLED=$(read_with_weight f4)
Q_WRAPPED="SELECT count(), max(finalizeAggregation(s))
    FROM file('${DATA_DIR}/f2.parquet', Parquet, 'x UInt32, s AggregateFunction(any, String)') WHERE x > 100000
    SETTINGS aggregate_function_input_format = 'value', use_query_condition_cache = 0, $PINNED FORMAT Null;"
Q_PLAIN="SELECT count() FROM file('${DATA_DIR}/f2.parquet', Parquet, 'x UInt32, s String') WHERE x > 100000
    SETTINGS use_query_condition_cache = 0, $PINNED FORMAT Null;"
Q_SKIPPED="SELECT ifNull((SELECT value FROM system.events WHERE event = 'ParquetFilesSkippedByCachedStatistics'), 0);"
read -r w1 w2 w3 skipped1 skipped2 u1 u2 <<< "$(${CLICKHOUSE_LOCAL} --query "$(read_columns_twice f1) $Q_LOCAL $Q_LOCAL
    $Q_SKIPPED $Q_WRAPPED $Q_WRAPPED $Q_PLAIN $Q_SKIPPED $(read_columns_twice f4) $Q_UNSETTLED" | tr '\n' ' ')"
echo "clickhouse-local: weight counts the kept ranges $((w2 > w1)), unchanged by the skip $((w3 == w2)), skipped $skipped1"
echo "clickhouse-local, kept through the aggregate function wrapper: skipped $((skipped2 - skipped1))"
echo "clickhouse-local, unsettled version token: weight unchanged by the filtered read $((u2 == u1))"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05390; DROP TABLE t_05390_i64"
rm -rf "${DATA_DIR:?}"

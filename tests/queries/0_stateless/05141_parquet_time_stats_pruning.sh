#!/usr/bin/env bash
# Tags: no-fasttest

# Parquet `TIME` is decoded as `Time64` (see `04277_parquet_time_of_day`), so the min/max
# statistics of the raw values describe the output values exactly and row group pruning must
# stay enabled for it. This is the positive counterpart of
# `05059_parquet_timestamp_to_time64_no_stats_pruning`, which pins that parquet `TIMESTAMP`
# read with a `Time64` hint must NOT be pruned by stats (that cast wraps by day).
#
# Both physical layouts of a time-of-day are covered: `TIME_MICROS` (INT64) and
# `TIME_MILLIS` (INT32).
#
# The ClickHouse parquet writer cannot write `Time64`, so the files are produced by pyarrow.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE_US=$CLICKHOUSE_TEST_UNIQUE_NAME.us.parquet
DATA_FILE_MS=$CLICKHOUSE_TEST_UNIQUE_NAME.ms.parquet

# 4 row groups of 100 rows each; row group N holds times of day in
# [(N + 1):00:00.000, (N + 1):00:00.099].
python3 -c "
import pyarrow as pa
import pyarrow.parquet as pq

hour = 3600 * 1000 * 1000
us = [(i // 100 + 1) * hour + (i % 100) * 1000 for i in range(400)]
pq.write_table(pa.table({'t': pa.array(us, type=pa.time64('us'))}), '$DATA_FILE_US', row_group_size=100)

hour_ms = 3600 * 1000
ms = [(i // 100 + 1) * hour_ms + (i % 100) for i in range(400)]
pq.write_table(pa.table({'t': pa.array(ms, type=pa.time32('ms'))}), '$DATA_FILE_MS', row_group_size=100)
"

run_and_report() {
    local file=$1
    local type=$2
    local predicate=$3

    echo "--- $type: $predicate"
    local out
    out=$($CLICKHOUSE_LOCAL --print-profile-events --input_format_parquet_filter_push_down 1 -q "
        SELECT count(), min(t), max(t) FROM file('$file', Parquet, 't $type') WHERE $predicate
    " 2>&1)
    # The query result, then the row group counters (the profile events are logged with a
    # timestamp prefix, the result is not).
    echo "$out" | grep -vE '^[0-9]{4}\.[0-9]{2}\.[0-9]{2} '
    echo "$out" | grep -oE '(ParquetReadRowGroups|ParquetPrunedRowGroups): [0-9]+' | sort
}

# TIME_MICROS, INT64 physical.
run_and_report "$DATA_FILE_US" "Time64(6)" "t >= toTime64('03:00:00', 6)"
run_and_report "$DATA_FILE_US" "Time64(6)" "t = toTime64('02:00:00.050', 6)"
run_and_report "$DATA_FILE_US" "Time64(6)" "t > toTime64('05:00:00', 6)"

# TIME_MILLIS, INT32 physical.
run_and_report "$DATA_FILE_MS" "Time64(3)" "t >= toTime64('03:00:00', 3)"
run_and_report "$DATA_FILE_MS" "Time64(3)" "t = toTime64('02:00:00.050', 3)"
run_and_report "$DATA_FILE_MS" "Time64(3)" "t > toTime64('05:00:00', 3)"

rm -f "$DATA_FILE_US" "$DATA_FILE_MS"

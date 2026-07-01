#!/usr/bin/env bash
# Tags: no-fasttest

# Test that Parquet `TIME` logical type (time-of-day) maps to ClickHouse Time64
# and is NOT affected by `session_timezone`.
#
# Before the fix, Parquet `TIME_MILLIS`/`TIME_MICROS` were routed through
# `DataTypeDateTime64`, which under a non-UTC `session_timezone` made the
# `DateTime64 -> Time64` cast offset the value by the timezone — the same
# silent corruption as Arrow's #104038.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.parquet

# Generate a Parquet file with all four Parquet `time`/`timestamp` precisions:
#   time32[ms]  -> TIME_MILLIS  (INT32 physical)
#   time64[us]  -> TIME_MICROS  (INT64 physical)
#   time64[ns]  -> TIME_NANOS   (INT64 physical)
#   timestamp[us] -> TIMESTAMP_MICROS (kept for non-Time path sanity)
python3 -c "
import pyarrow as pa
import pyarrow.parquet as pq

table = pa.table({
    't_ms': pa.array([3723456],          type=pa.time32('ms')),     # 01:02:03.456
    't_us': pa.array([3723456789],       type=pa.time64('us')),     # 01:02:03.456789
    't_ns': pa.array([3723456789012],    type=pa.time64('ns')),     # 01:02:03.456789012
    'ts_us': pa.array([1700000000000000],type=pa.timestamp('us', tz='UTC')),
})
pq.write_table(table, '$DATA_FILE')
"

echo "=== Schema-less inference ==="
# Without explicit schema, Parquet TIME columns must now be inferred as Time64
# (was DateTime64 before the fix).
$CLICKHOUSE_LOCAL -q "DESCRIBE TABLE file('$DATA_FILE', 'Parquet') FORMAT TSV" \
    | cut -f1,2

echo "=== Import into Time64 columns (session_timezone = Asia/Shanghai) ==="
# Importing into Time64 columns under a non-UTC session_timezone must not shift
# the stored values.
$CLICKHOUSE_LOCAL --session_timezone 'Asia/Shanghai' -q "
    CREATE OR REPLACE TABLE test_parquet_time (
        t_ms Time64(3),
        t_us Time64(6),
        t_ns Time64(9)
    ) ENGINE = Memory;

    INSERT INTO test_parquet_time (t_ms, t_us, t_ns)
        SELECT t_ms, t_us, t_ns FROM file('$DATA_FILE', 'Parquet');
    SELECT * FROM test_parquet_time;
"

echo "=== Explicit type hints under non-UTC session_timezone ==="
# Read the same TIME_MILLIS column (Parquet value = 3723456 ms = 01:02:03.456)
# with several explicit type hints and verify every path stays correct under a
# non-UTC session_timezone.
#   - Time64(3)              -> 01:02:03.456    (direct, no cast)
#   - Time                   -> 01:02:03        (Time64 -> Time cast, seconds precision)
#   - DateTime('UTC')        -> 1970-01-01 01:02:03  (Time64 -> DateTime cast, legacy 00900 path)
#   - DateTime64(3, 'UTC')   -> 1970-01-01 01:02:03.456  (Time64 -> DateTime64 with explicit tz)
#   - Int64                  -> 3723            (Time64 -> Int64, Decimal->Int discards scale)
$CLICKHOUSE_LOCAL --session_timezone 'Asia/Shanghai' -q "
    SELECT 'Time64(3)' AS hint, toString(t_ms) AS v
        FROM file('$DATA_FILE', 'Parquet', 't_ms Time64(3)');
    SELECT 'Time' AS hint, toString(t_ms) AS v
        FROM file('$DATA_FILE', 'Parquet', 't_ms Time');
    SELECT 'DateTime(UTC)' AS hint, toString(t_ms) AS v
        FROM file('$DATA_FILE', 'Parquet', \$\$t_ms DateTime('UTC')\$\$);
    SELECT 'DateTime64(3, UTC)' AS hint, toString(t_ms) AS v
        FROM file('$DATA_FILE', 'Parquet', \$\$t_ms DateTime64(3, 'UTC')\$\$);
    SELECT 'Int64' AS hint, toString(t_ms) AS v
        FROM file('$DATA_FILE', 'Parquet', 't_ms Int64');
"

# Cleanup
rm -f "$DATA_FILE"

#!/usr/bin/env bash
# Tags: no-fasttest

# Test that Avro `time-millis` / `time-micros` logical types round-trip through
# ClickHouse `Time64`, and that ClickHouse `Time` / `Time64` are written out
# with the matching Avro logical type.
#
# Before the fix, Avro's TIME_* logical types were ignored on read (the column
# became a plain Int32 / Int64) and on write (Time / Time64 hit the writer's
# unsupported-type path and threw ILLEGAL_COLUMN), so the wall-clock intent
# was lost in every round-trip with another Avro consumer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.avro

# Use ClickHouse itself to produce the Avro fixture. After this PR the writer
# emits `time-millis` (INT32) for `Time` / `Time64(3)` and `time-micros` (LONG)
# for `Time64(6)`. We probe the binary later with grep to confirm the logical
# type strings actually made it into the schema header, then exercise the
# reader paths against the same file.
$CLICKHOUSE_LOCAL -q "
    SELECT
        toTime64('01:02:03.456',     3) AS t_ms,
        toTime64('01:02:03.456789',  6) AS t_us
    FORMAT Avro
    SETTINGS output_format_avro_codec = 'null'
" > "$DATA_FILE"

echo "=== Writer: Avro schema header contains time-millis / time-micros logical types ==="
# Avro IPC files embed the JSON schema (uncompressed in this test) near the
# start of the file, so a plain text scan reveals the emitted logical types.
grep -aoE 'time-(millis|micros|nanos)' "$DATA_FILE" | sort -u

echo "=== Schema-less inference ==="
# Reader: Avro time-millis -> Time64(3); time-micros -> Time64(6).
$CLICKHOUSE_LOCAL -q "DESCRIBE TABLE file('$DATA_FILE', 'Avro') FORMAT TSV" \
    | cut -f1,2

echo "=== Schema-less read (session_timezone = Asia/Shanghai) ==="
# Avro time-of-day is timezone-unaware, ClickHouse Time64 is timezone-unaware,
# so a non-UTC session must not shift the value.
$CLICKHOUSE_LOCAL --session_timezone 'Asia/Shanghai' \
    -q "SELECT t_ms, t_us FROM file('$DATA_FILE', 'Avro')"

echo "=== INSERT into matching-scale Time64 columns (session_timezone = Asia/Shanghai) ==="
$CLICKHOUSE_LOCAL --session_timezone 'Asia/Shanghai' -mn -q "
    CREATE OR REPLACE TABLE test_avro_time (
        t_ms Time64(3),
        t_us Time64(6)
    ) ENGINE = Memory;

    INSERT INTO test_avro_time SELECT t_ms, t_us FROM file('$DATA_FILE', 'Avro');
    SELECT * FROM test_avro_time;
"

echo "=== Explicit type hints with scale conversion ==="
# t_ms (TIME_MILLIS, ms-since-midnight) read into a variety of target types:
# the reader must rescale ms-resolution to the requested target scale.
$CLICKHOUSE_LOCAL --session_timezone 'Asia/Shanghai' -mn -q "
    SELECT 'TIME_MILLIS -> Time      ' AS k, toString(t_ms) FROM file('$DATA_FILE', 'Avro', 't_ms Time, t_us Time64(6)');
    SELECT 'TIME_MILLIS -> Time64(0) ' AS k, toString(t_ms) FROM file('$DATA_FILE', 'Avro', 't_ms Time64(0), t_us Time64(6)');
    SELECT 'TIME_MILLIS -> Time64(6) ' AS k, toString(t_ms) FROM file('$DATA_FILE', 'Avro', 't_ms Time64(6), t_us Time64(6)');
    SELECT 'TIME_MILLIS -> Time64(9) ' AS k, toString(t_ms) FROM file('$DATA_FILE', 'Avro', 't_ms Time64(9), t_us Time64(6)');
"
# t_us (TIME_MICROS, us-since-midnight) read into several target types.
$CLICKHOUSE_LOCAL --session_timezone 'Asia/Shanghai' -mn -q "
    SELECT 'TIME_MICROS -> Time      ' AS k, toString(t_us) FROM file('$DATA_FILE', 'Avro', 't_ms Time64(3), t_us Time');
    SELECT 'TIME_MICROS -> Time64(0) ' AS k, toString(t_us) FROM file('$DATA_FILE', 'Avro', 't_ms Time64(3), t_us Time64(0)');
    SELECT 'TIME_MICROS -> Time64(3) ' AS k, toString(t_us) FROM file('$DATA_FILE', 'Avro', 't_ms Time64(3), t_us Time64(3)');
    SELECT 'TIME_MICROS -> Time64(9) ' AS k, toString(t_us) FROM file('$DATA_FILE', 'Avro', 't_ms Time64(3), t_us Time64(9)');
"

echo "=== Writer covers ClickHouse Time type as well ==="
# Time (Int32 seconds) -> Avro time-millis (multiplied by 1000).
$CLICKHOUSE_LOCAL -q "
    SELECT '12:34:56'::Time AS t_s
    FORMAT Avro
    SETTINGS output_format_avro_codec = 'null'
" > "$DATA_FILE"
grep -aoE 'time-(millis|micros|nanos)' "$DATA_FILE" | sort -u
$CLICKHOUSE_LOCAL -q "DESCRIBE TABLE file('$DATA_FILE', 'Avro') FORMAT TSV" | cut -f1,2
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE', 'Avro')"

rm -f "$DATA_FILE"

#!/usr/bin/env bash
# Tags: no-fasttest

# Test that Avro `time-millis` / `time-micros` logical types round-trip through
# ClickHouse `Time64`, and that ClickHouse `Time` / `Time64` are written out
# with the matching Avro logical type.
#
# Before the fix, Avro's TIME_* logical types were ignored on read (the column
# became a plain Int32 / Int64) and on write (Time / Time64 were emitted as
# raw decimals without any logical type), so the wall-clock intent was lost in
# every round-trip with another Avro consumer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.avro

# Generate an Avro file with both Avro time logical types:
#   time-millis (INT32) : 3723456 ms      = 01:02:03.456
#   time-micros (INT64) : 3723456789 us   = 01:02:03.456789
python3 -c "
import fastavro
schema = {
    'type': 'record', 'name': 'r',
    'fields': [
        {'name': 't_ms', 'type': {'type': 'int',  'logicalType': 'time-millis'}},
        {'name': 't_us', 'type': {'type': 'long', 'logicalType': 'time-micros'}},
    ],
}
with open('$DATA_FILE', 'wb') as f:
    fastavro.writer(f, schema, [{'t_ms': 3723456, 't_us': 3723456789}])
"

echo "=== Schema-less inference ==="
# Avro time-millis -> Time64(3); time-micros -> Time64(6).
$CLICKHOUSE_LOCAL -q "DESCRIBE TABLE file('$DATA_FILE', 'Avro') FORMAT TSV" \
    | cut -f1,2

echo "=== Schema-less read (session_timezone = Asia/Shanghai) ==="
# session_timezone is non-UTC and must not shift the stored values.
$CLICKHOUSE_LOCAL --session_timezone 'Asia/Shanghai' \
    -q "SELECT t_ms, t_us FROM file('$DATA_FILE', 'Avro')"

echo "=== INSERT into matching-scale Time64 columns (session_timezone = Asia/Shanghai) ==="
# Same-scale INSERT from Avro file: column type is preserved end-to-end.
$CLICKHOUSE_LOCAL --session_timezone 'Asia/Shanghai' -mn -q "
    CREATE OR REPLACE TABLE test_avro_time (
        t_ms Time64(3),
        t_us Time64(6)
    ) ENGINE = Memory;

    INSERT INTO test_avro_time SELECT t_ms, t_us FROM file('$DATA_FILE', 'Avro');
    SELECT * FROM test_avro_time;
"

# ---------------------------------------------------------------
# Writer: ClickHouse Time / Time64(3) / Time64(6) -> Avro TIME_*.
# Verify both the schema (logical type + physical type) and the values.
# ---------------------------------------------------------------
echo "=== Writer: ClickHouse Time / Time64 -> Avro TIME_* (schema check) ==="
$CLICKHOUSE_LOCAL -q "
    SELECT
        '12:34:56'::Time                          AS t_s,
        toTime64('01:02:03.456', 3)               AS t_ms64,
        toTime64('01:02:03.456789', 6)            AS t_us64
    FORMAT Avro
    SETTINGS output_format_avro_codec = 'null'
" > "$DATA_FILE"

python3 -c "
import fastavro
with open('$DATA_FILE', 'rb') as f:
    r = fastavro.reader(f)
    schema = r.writer_schema
    fields = {field['name']: field['type'] for field in schema['fields']}
    assert fields['t_s']    == {'logicalType': 'time-millis', 'type': 'int'},  f't_s: {fields[\"t_s\"]}'
    assert fields['t_ms64'] == {'logicalType': 'time-millis', 'type': 'int'},  f't_ms64: {fields[\"t_ms64\"]}'
    assert fields['t_us64'] == {'logicalType': 'time-micros', 'type': 'long'}, f't_us64: {fields[\"t_us64\"]}'
    print('schema OK')
"

echo "=== Writer round-trip via ClickHouse read-back ==="
# Read the just-written Avro file back and verify the values and inferred
# ClickHouse types.
$CLICKHOUSE_LOCAL -q "DESCRIBE TABLE file('$DATA_FILE', 'Avro') FORMAT TSV" \
    | cut -f1,2
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE', 'Avro')"

rm -f "$DATA_FILE"

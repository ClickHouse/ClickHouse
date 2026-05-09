#!/usr/bin/env bash
# Tags: no-fasttest

# Test correct mapping of Arrow time32/time64 to ClickHouse Time/Time64,
# timezone independence during import, and proper export roundtrip.
#
# https://github.com/ClickHouse/ClickHouse/issues/104038
#
#   - Import: Arrow time32/time64 → Time64 (timezone must NOT affect values)
#   - Export: ClickHouse Time/Time64 → Arrow (exact type & value verified by Python)
#
# Mapping reference:
#   ClickHouse Time        → Arrow time32[s]
#   ClickHouse Time64(0)   → Arrow time32[s]
#   ClickHouse Time64(1..3) → Arrow time32[ms] (value scaled to milliseconds)
#   ClickHouse Time64(4..6) → Arrow time64[us] (value scaled to microseconds)
#   ClickHouse Time64(7..9) → Arrow time64[ns] (value scaled to nanoseconds)
#   Arrow time32/time64 → ClickHouse Time64 (with corresponding precision)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.arrow

# ---------------------------------------------------------------
# 1. Import: Arrow time32/time64 -> ClickHouse Time/Time64,
#    verify that session_timezone does NOT change the stored value.
# ---------------------------------------------------------------
echo "=== Import Arrow time types (session_timezone = Asia/Shanghai) ==="

# Generate an Arrow file with:
#   time32[s]  : 01:02:03
#   time32[ms] : 01:02:03.456
#   time64[us] : 01:02:03.456789
#   time64[ns] : 01:02:03.456789012
python3 -c "
import pyarrow as pa
import pyarrow.feather as feather

table = pa.table({
    't_s':  pa.array([3723],          type=pa.time32('s')),
    't_ms': pa.array([3723456],       type=pa.time32('ms')),
    't_us': pa.array([3723456789],    type=pa.time64('us')),
    't_ns': pa.array([3723456789012], type=pa.time64('ns'))
})
feather.write_feather(table, '$DATA_FILE', compression='uncompressed')
"

# Insert into Time64 columns with a non-UTC timezone.
$CLICKHOUSE_LOCAL --session_timezone 'Asia/Shanghai' -q "
    CREATE OR REPLACE TABLE test_time_import (
        t_s  Time64(0),   -- should accept time32[s]
        t_ms Time64(3),   -- should accept time32[ms]
        t_us Time64(6),   -- should accept time64[us]
        t_ns Time64(9)    -- should accept time64[ns]
    ) ENGINE = Memory;

    INSERT INTO test_time_import FROM INFILE '$DATA_FILE' FORMAT Arrow;
    SELECT * FROM test_time_import;
"
# Expected output (no timezone shift):
# 01:02:03	01:02:03.456	01:02:03.456789	01:02:03.456789012

# ---------------------------------------------------------------
# 2. Export: ClickHouse Time/Time64 -> Arrow, verify types & values
# ---------------------------------------------------------------
echo "=== Export Time64 to Arrow – verify via Python ==="

# Export a set of values covering all precision boundaries.
$CLICKHOUSE_LOCAL -q "
    SELECT
        toTime64('01:02:03', 0)               AS t0,
        toTime64('01:02:03.4', 1)             AS t1,
        toTime64('01:02:03.45', 2)            AS t2,
        toTime64('01:02:03.456', 3)           AS t3,
        toTime64('01:02:03.4567', 4)          AS t4,
        toTime64('01:02:03.45678', 5)         AS t5,
        toTime64('01:02:03.456789', 6)        AS t6,
        toTime64('01:02:03.4567890', 7)       AS t7,
        toTime64('01:02:03.45678901', 8)      AS t8,
        toTime64('01:02:03.456789012', 9)     AS t9
    FORMAT Arrow
" >"$DATA_FILE"

python3 -c "
import pyarrow as pa
import pyarrow.feather as feather

table = feather.read_table('$DATA_FILE')
schema = table.schema

# Check Arrow types
assert schema.field('t0').type == pa.time32('s'), f't0: expected time32[s], got {schema.field(\"t0\").type}'
assert schema.field('t1').type == pa.time32('ms'), f't1: expected time32[ms], got {schema.field(\"t1\").type}'
assert schema.field('t2').type == pa.time32('ms'), f't2: expected time32[ms], got {schema.field(\"t2\").type}'
assert schema.field('t3').type == pa.time32('ms'), f't3: expected time32[ms], got {schema.field(\"t3\").type}'
assert schema.field('t4').type == pa.time64('us'), f't4: expected time64[us], got {schema.field(\"t4\").type}'
assert schema.field('t5').type == pa.time64('us'), f't5: expected time64[us], got {schema.field(\"t5\").type}'
assert schema.field('t6').type == pa.time64('us'), f't6: expected time64[us], got {schema.field(\"t6\").type}'
assert schema.field('t7').type == pa.time64('ns'), f't7: expected time64[ns], got {schema.field(\"t7\").type}'
assert schema.field('t8').type == pa.time64('ns'), f't8: expected time64[ns], got {schema.field(\"t8\").type}'
assert schema.field('t9').type == pa.time64('ns'), f't9: expected time64[ns], got {schema.field(\"t9\").type}'
"

# Also verify that ClickHouse can read its own Arrow output back correctly.
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE', 'Arrow')"

# Do the same for the `Time` type (seconds precision only).
echo "=== Export Time type to Arrow ==="
$CLICKHOUSE_LOCAL -q "SELECT '12:34:56'::Time AS t_s FORMAT Arrow" >"$DATA_FILE"

python3 -c "
import pyarrow as pa
import pyarrow.feather as feather

table = feather.read_table('$DATA_FILE')
schema = table.schema
assert schema.field('t_s').type == pa.time32('s'), f't_s: expected time32[s], got {schema.field(\"t_s\").type}'
"

$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE', 'Arrow')"

# ---------------------------------------------------------------
# 3. Overflow
# ---------------------------------------------------------------
echo "=== Export Time64 to Arrow – Overflow ==="

$CLICKHOUSE_LOCAL -q "SELECT toTime64(0, 3) + INTERVAL 1000 YEAR FORMAT Arrow" >"$DATA_FILE"
$CLICKHOUSE_LOCAL -q "SELECT toTime64(0, 3) - INTERVAL 1000 YEAR FORMAT Arrow" >"$DATA_FILE"

# Cleanup
rm -f "$DATA_FILE"

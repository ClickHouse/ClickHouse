#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: the two replicas are created with explicit paths and replica names.

# Parts moved around whole: fetched by a second replica, the type with SHARED set through Keeper, backed up and restored,
# detached and attached as a table, a partition and a part, and moved between tables, with the plain type rejected. Every
# read is compared with a Memory table of the type without SHARED.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SHARED="JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8))"
PLAIN="JSON(a UInt64, b String, c Array(UInt8))"
ROW="concat('{\"a\":', toString(number * 10), ',\"b\":\"', repeat('x', number % 7), '\",\"c\":', toString(range(number % 4)), if(number % 2 = 0, concat(',\"d\":', toString(number)), ''), '}')"
WIDE="min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1"
COMPACT="min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4"

reads() # table, label, [where]: the whole column, a path, a subcolumn and a pair compared with the Memory twin
{
    local table=$1 label=$2 where=${3:-} q=""
    for select in json json.a json.c.size0 "(json.b, json.a)"; do
        q+="SELECT '$label $select', (SELECT arraySort(groupArray((x, toString($select)))) FROM $table $where SETTINGS max_threads = 1, max_block_size = 7) = (SELECT arraySort(groupArray((x, toString($select)))) FROM mem $where);"
    done
    $CLICKHOUSE_CLIENT -n -q "$q"
}

$CLICKHOUSE_CLIENT -n -q "DROP TABLE IF EXISTS mem; CREATE TABLE mem (x UInt64, json $PLAIN) ENGINE = Memory; INSERT INTO mem SELECT number, $ROW FROM numbers(40);"

echo "-- two replicas: a Wide part fetched by the second replica"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS r1; DROP TABLE IF EXISTS r2;
CREATE TABLE r1 (x UInt64, json $SHARED) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/r', 'r1') ORDER BY x SETTINGS $WIDE;
CREATE TABLE r2 (x UInt64, json $SHARED) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/r', 'r2') ORDER BY x SETTINGS $WIDE;
INSERT INTO r1 SELECT number, $ROW FROM numbers(40);
SYSTEM SYNC REPLICA r2;
SELECT 'r2 parts', part_type, count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'r2' AND active GROUP BY part_type;
"
reads r2 "r2"
reads r2 "r2 WHERE x >= 9 AND x < 30" "WHERE x >= 9 AND x < 30"
$CLICKHOUSE_CLIENT -n -q "
CHECK TABLE r1 SETTINGS check_query_single_value_result = 1;
CHECK TABLE r2 SETTINGS check_query_single_value_result = 1;
DROP TABLE r1; DROP TABLE r2;"

echo "-- the type with SHARED set lazily through Keeper, Compact parts of both layouts on both replicas"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS p1; DROP TABLE IF EXISTS p2;
CREATE TABLE p1 (x UInt64, json $PLAIN) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/p', 'r1') ORDER BY x SETTINGS $COMPACT;
CREATE TABLE p2 (x UInt64, json $PLAIN) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/p', 'r2') ORDER BY x SETTINGS $COMPACT;
INSERT INTO p1 SELECT number, $ROW FROM numbers(20);
ALTER TABLE p1 MODIFY COLUMN json $SHARED SETTINGS enable_json_lazy_type_hints = 1, alter_sync = 2;
SELECT 'p type on both replicas', (SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'p1' AND name = 'json'), (SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'p2' AND name = 'json');
INSERT INTO p2 SELECT number, $ROW FROM numbers(20, 20);
SYSTEM SYNC REPLICA p1;
SELECT 'p1 layout', rows, arrayExists(s -> s LIKE '%object_typed_shared_data%', substreams) FROM system.parts_columns WHERE database = currentDatabase() AND table = 'p1' AND column = 'json' AND active ORDER BY name;
"
reads p1 "p1"
reads p2 "p2"
$CLICKHOUSE_CLIENT -n -q "
CHECK TABLE p1 SETTINGS check_query_single_value_result = 1;
CHECK TABLE p2 SETTINGS check_query_single_value_result = 1;
DROP TABLE p1; DROP TABLE p2;"

echo "-- BACKUP and RESTORE, both part types"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS b_w; DROP TABLE IF EXISTS b_c; DROP TABLE IF EXISTS rb_w; DROP TABLE IF EXISTS rb_c;
CREATE TABLE b_w (x UInt64, json $SHARED) ENGINE = MergeTree ORDER BY x SETTINGS $WIDE;
CREATE TABLE b_c (x UInt64, json $SHARED) ENGINE = MergeTree ORDER BY x SETTINGS $COMPACT;
INSERT INTO b_w SELECT number, $ROW FROM numbers(40);
INSERT INTO b_c SELECT number, $ROW FROM numbers(40);
BACKUP TABLE b_w, TABLE b_c TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}') FORMAT Null;
RESTORE TABLE b_w AS rb_w, TABLE b_c AS rb_c FROM Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}') FORMAT Null;
SELECT 'restored substreams equal',
    (SELECT groupArray((part_type, substreams)) FROM system.parts_columns WHERE database = currentDatabase() AND table = 'rb_w' AND column = 'json' AND active) = (SELECT groupArray((part_type, substreams)) FROM system.parts_columns WHERE database = currentDatabase() AND table = 'b_w' AND column = 'json' AND active),
    (SELECT groupArray((part_type, substreams)) FROM system.parts_columns WHERE database = currentDatabase() AND table = 'rb_c' AND column = 'json' AND active) = (SELECT groupArray((part_type, substreams)) FROM system.parts_columns WHERE database = currentDatabase() AND table = 'b_c' AND column = 'json' AND active);
"
reads rb_w "rb_w"
reads rb_c "rb_c"
$CLICKHOUSE_CLIENT -n -q "
CHECK TABLE rb_w SETTINGS check_query_single_value_result = 1;
CHECK TABLE rb_c SETTINGS check_query_single_value_result = 1;
DROP TABLE b_w; DROP TABLE b_c; DROP TABLE rb_w; DROP TABLE rb_c;"

echo "-- DETACH and ATTACH: the table, a partition, a part"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS d;
CREATE TABLE d (x UInt64, json $SHARED) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x SETTINGS $WIDE;
INSERT INTO d SELECT number, $ROW FROM numbers(20);
INSERT INTO d SELECT number, $ROW FROM numbers(20, 20);
DETACH TABLE d;
ATTACH TABLE d;
SELECT 'd attached table parts', count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'd' AND active;
"
reads d "d attached table"
$CLICKHOUSE_CLIENT -n -q "
ALTER TABLE d DETACH PARTITION 1;
SELECT 'd without partition 1', count(), sum(json.a) FROM d;
ALTER TABLE d ATTACH PARTITION 1;
"
reads d "d attached partition"
part=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'd' AND active AND partition = '0' ORDER BY name LIMIT 1")
$CLICKHOUSE_CLIENT -n -q "
ALTER TABLE d DETACH PART '$part';
SELECT 'd without a part', count() FROM d;
ALTER TABLE d ATTACH PART '$part';
"
reads d "d attached part"
$CLICKHOUSE_CLIENT -q "CHECK TABLE d SETTINGS check_query_single_value_result = 1"

echo "-- MOVE PARTITION between tables of the same type; the plain type rejected"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS d2; DROP TABLE IF EXISTS dp;
CREATE TABLE d2 (x UInt64, json $SHARED) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x SETTINGS $WIDE;
CREATE TABLE dp (x UInt64, json $PLAIN) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x SETTINGS $WIDE;
ALTER TABLE d MOVE PARTITION 0 TO TABLE d2;
ALTER TABLE d MOVE PARTITION 1 TO TABLE d2;
SELECT 'd after the move', count() FROM d;
"
reads d2 "d2 moved"
$CLICKHOUSE_CLIENT -q "ALTER TABLE d2 MOVE PARTITION 0 TO TABLE dp" 2>&1 | grep -o "Code: [0-9]*" | head -1
$CLICKHOUSE_CLIENT -q "ALTER TABLE dp ATTACH PARTITION 0 FROM d2" 2>&1 | grep -o "Code: [0-9]*" | head -1
$CLICKHOUSE_CLIENT -n -q "
SELECT 'dp untouched', count() FROM dp;
CHECK TABLE d2 SETTINGS check_query_single_value_result = 1;
DROP TABLE d; DROP TABLE d2; DROP TABLE dp; DROP TABLE mem;"

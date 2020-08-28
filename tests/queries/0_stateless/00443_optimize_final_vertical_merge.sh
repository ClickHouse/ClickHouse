#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

db="test"
table="optimize_me_finally"
name="$db.$table"
res_rows=1500000 # >= vertical_merge_algorithm_min_rows_to_activate

function get_num_parts {
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE active AND database='$db' AND table='$table'"
}

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $name"

$CLICKHOUSE_CLIENT -q "CREATE TABLE $name (
date Date,
Sign Int8,
ki UInt64,

ds String,
di01 UInt64,
di02 UInt64,
di03 UInt64,
di04 UInt64,
di05 UInt64,
di06 UInt64,
di07 UInt64,
di08 UInt64,
di09 UInt64,
di10 UInt64,
n Nested(
i UInt64,
s String
)
)
ENGINE = CollapsingMergeTree(date, (date, ki), 8192, Sign)"

$CLICKHOUSE_CLIENT -q "INSERT INTO $name (date, Sign, ki) SELECT
toDate(0) AS date,
toInt8(1) AS Sign,
toUInt64(0) AS ki
FROM system.numbers LIMIT 9000"

$CLICKHOUSE_CLIENT -q "INSERT INTO $name (date, Sign, ki) SELECT
toDate(0) AS date,
toInt8(1) AS Sign,
number AS ki
FROM system.numbers LIMIT 9000, 9000"

$CLICKHOUSE_CLIENT -q "INSERT INTO $name SELECT
toDate(0) AS date,
toInt8(1) AS Sign,
number AS ki,
hex(number) AS ds,
number AS di01,
number AS di02,
number AS di03,
number AS di04,
number AS di05,
number AS di06,
number AS di07,
number AS di08,
number AS di09,
number AS di10,
[number, number+1] AS \`n.i\`,
[hex(number), hex(number+1)] AS \`n.s\`
FROM system.numbers LIMIT $res_rows"

while [[ $(get_num_parts) -ne 1 ]] ; do $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE $name PARTITION 197001" --server_logs_file=/dev/null; done

$CLICKHOUSE_CLIENT -q "ALTER TABLE $name ADD COLUMN n.a Array(String)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE $name ADD COLUMN da Array(String) DEFAULT ['def']"

$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE $name PARTITION 197001 FINAL" --server_logs_file=/dev/null

$CLICKHOUSE_CLIENT -q "ALTER TABLE $name MODIFY COLUMN n.a Array(String) DEFAULT ['zzz']"
$CLICKHOUSE_CLIENT -q "ALTER TABLE $name MODIFY COLUMN da Array(String) DEFAULT ['zzz']"

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(Sign), sum(ki = di05), sum(hex(ki) = ds), sum(ki = n.i[1]), sum([hex(ki), hex(ki+1)] = n.s) FROM $name"
$CLICKHOUSE_CLIENT -q "SELECT groupUniqArray(da), groupUniqArray(n.a) FROM $name"

hash_src=$($CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT cityHash64(groupArray(ki)) FROM $name")
hash_ref=$($CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT cityHash64(groupArray(ki)) FROM (SELECT number as ki FROM system.numbers LIMIT $res_rows)")
echo $(( $hash_src - $hash_ref ))

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $name"

#!/usr/bin/env bash
set -e

db="test"
table="optimize_me_finally"
name="$db.$table"
res_rows=1500000 # >= vertical_merge_algorithm_min_rows_to_activate

function get_num_parts {
    clickhouse-client -q "SELECT count() FROM system.parts WHERE active AND database='$db' AND table='$table'"
}

clickhouse-client -q "DROP TABLE IF EXISTS $name"

clickhouse-client -q "CREATE TABLE $name (
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

clickhouse-client -q "INSERT INTO $name (date, Sign, ki) SELECT
toDate(0) AS date,
toInt8(1) AS Sign,
toUInt64(0) AS ki
FROM system.numbers LIMIT 9000"

clickhouse-client -q "INSERT INTO $name (date, Sign, ki) SELECT
toDate(0) AS date,
toInt8(1) AS Sign,
number AS ki
FROM system.numbers LIMIT 9000, 9000"

clickhouse-client -q "INSERT INTO $name SELECT
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

while [[ `get_num_parts` -ne 1 ]] ; do clickhouse-client -q "OPTIMIZE TABLE $name PARTITION 197001"; done

clickhouse-client -q "ALTER TABLE $name ADD COLUMN n.a Array(String)"
clickhouse-client -q "ALTER TABLE $name ADD COLUMN da Array(String) DEFAULT ['def']"

clickhouse-client -q "OPTIMIZE TABLE $name PARTITION 197001 FINAL"

clickhouse-client -q "ALTER TABLE $name MODIFY COLUMN n.a Array(String) DEFAULT ['zzz']"
clickhouse-client -q "ALTER TABLE $name MODIFY COLUMN da Array(String) DEFAULT ['zzz']"

clickhouse-client -q "SELECT count(), sum(Sign), sum(ki = di05), sum(hex(ki) = ds), sum(ki = n.i[1]), sum([hex(ki), hex(ki+1)] = n.s) FROM $name"
clickhouse-client -q "SELECT groupUniqArray(da), groupUniqArray(n.a) FROM $name"

hash_src=`clickhouse-client --max_threads=1 -q "SELECT cityHash64(groupArray(ki)) FROM $name"`
hash_ref=`clickhouse-client --max_threads=1 -q "SELECT cityHash64(groupArray(ki)) FROM (SELECT number as ki FROM system.numbers LIMIT $res_rows)"`
echo $(( $hash_src - $hash_ref ))

clickhouse-client -q "DROP TABLE IF EXISTS $name"

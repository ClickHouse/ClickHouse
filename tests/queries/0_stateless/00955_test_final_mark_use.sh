#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="
CREATE TABLE mt_with_pk (
  d Date DEFAULT '2000-01-01',
  x DateTime,
  y Array(UInt64),
  z UInt64,
  n Nested (Age UInt8, Name String),
  w Int16 DEFAULT 10
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(d) ORDER BY (x, z) SETTINGS index_granularity_bytes=10000, write_final_mark=1;"

$CLICKHOUSE_CLIENT --query="INSERT INTO mt_with_pk (d, x, y, z, n.Age, n.Name) VALUES (toDate('2018-10-01'), toDateTime('2018-10-01 12:57:57'), [1, 1, 1], 11, [77], ['Joe']), (toDate('2018-10-01'), toDateTime('2018-10-01 16:57:57'), [2, 2, 2], 12, [88], ['Mark']), (toDate('2018-10-01'), toDateTime('2018-10-01 19:57:57'), [3, 3, 3], 13, [99], ['Robert']);"

$CLICKHOUSE_CLIENT --query="SELECT sum(marks) FROM system.parts WHERE table = 'mt_with_pk' AND database = currentDatabase() AND active=1;"

$CLICKHOUSE_CLIENT --query="SELECT COUNT(*) FROM mt_with_pk WHERE x > toDateTime('2018-10-01 23:57:57') FORMAT JSON;" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="INSERT INTO mt_with_pk (d, x, y, z, n.Age, n.Name) VALUES (toDate('2018-10-01'), toDateTime('2018-10-01 07:57:57'), [4, 4, 4], 14, [111, 222], ['Lui', 'Dave']), (toDate('2018-10-01'), toDateTime('2018-10-01 08:57:57'), [5, 5, 5], 15, [333, 444], ['John', 'Mike']), (toDate('2018-10-01'), toDateTime('2018-10-01 09:57:57'), [6, 6, 6], 16, [555, 666, 777], ['Alex', 'Jim', 'Tom']);"

$CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE mt_with_pk FINAL"

$CLICKHOUSE_CLIENT --query="SELECT sum(marks) FROM system.parts WHERE table = 'mt_with_pk' AND database = currentDatabase() AND active=1;"

$CLICKHOUSE_CLIENT --query="SELECT COUNT(*) FROM mt_with_pk WHERE x > toDateTime('2018-10-01 23:57:57') FORMAT JSON;" | grep "rows_read"

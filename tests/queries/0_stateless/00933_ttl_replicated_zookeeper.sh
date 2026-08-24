#!/usr/bin/env bash
# Tags: replica

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ttl_repl1"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ttl_repl2"

$CLICKHOUSE_CLIENT --query="CREATE TABLE ttl_repl1(d Date, x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00933/ttl_repl', '1') PARTITION BY toDayOfMonth(d) ORDER BY x TTL d + INTERVAL 1 DAY;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE ttl_repl2(d Date, x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00933/ttl_repl', '2') PARTITION BY toDayOfMonth(d) ORDER BY x TTL d + INTERVAL 1 DAY;"

$CLICKHOUSE_CLIENT --query="INSERT INTO TABLE ttl_repl1 VALUES (toDate('2000-10-10 00:00:00'), 100)"
$CLICKHOUSE_CLIENT --query="INSERT INTO TABLE ttl_repl1 VALUES (toDate('2100-10-10 00:00:00'), 200)"

$CLICKHOUSE_CLIENT --query="ALTER TABLE ttl_repl1 MODIFY TTL d + INTERVAL 1 DAY"
$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA ttl_repl2"

$CLICKHOUSE_CLIENT --query="INSERT INTO TABLE ttl_repl1 VALUES (toDate('2000-10-10 00:00:00'), 300)"
$CLICKHOUSE_CLIENT --query="INSERT INTO TABLE ttl_repl1 VALUES (toDate('2100-10-10 00:00:00'), 400)"

$CLICKHOUSE_CLIENT --query="SYSTEM SYNC REPLICA ttl_repl2"

query_with_retry "OPTIMIZE TABLE ttl_repl2 FINAL SETTINGS optimize_throw_if_noop = 1"

$CLICKHOUSE_CLIENT --query="SELECT x FROM ttl_repl2 ORDER BY x"

$CLICKHOUSE_CLIENT --query="SHOW CREATE TABLE ttl_repl2"

$CLICKHOUSE_CLIENT --query="DROP TABLE ttl_repl1"
$CLICKHOUSE_CLIENT --query="DROP TABLE ttl_repl2"

#!/usr/bin/env bash
# Tags: no-ordinary-database, zookeeper, no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


rand_id=$RANDOM;

$CLICKHOUSE_CLIENT --query="CREATE TABLE IF NOT EXISTS ds_data ENGINE=MergeTree ORDER BY number AS SELECT number FROM numbers(10000000);"
$CLICKHOUSE_CLIENT --query="CREATE TABLE IF NOT EXISTS ds_landing_$rand_id ( number UInt64 ) ENGINE = Null;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE IF NOT EXISTS ds_aux ( number UInt64, block_timestamp DateTime64(3) ) ENGINE = MergeTree ORDER BY number;"

$CLICKHOUSE_CLIENT --query="CREATE MATERIALIZED VIEW IF NOT EXISTS mv TO ds_aux as SELECT number, now64(3) block_timestamp FROM ds_landing_$rand_id;"

$CLICKHOUSE_CLIENT --query="INSERT INTO ds_landing_$rand_id SELECT * FROM ds_data SETTINGS max_insert_threads=16;"
sleep 0.5

$CLICKHOUSE_CLIENT --query="SELECT COUNT() FROM system.query_views_log WHERE initial_query_id = (SELECT initial_query_id FROM system.query_log WHERE query like '%INSERT INTO ds_landing_$rand_id%' LIMIT 1)"
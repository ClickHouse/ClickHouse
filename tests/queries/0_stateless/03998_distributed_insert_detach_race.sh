#!/usr/bin/env bash
# Tags: long

# Stress test for the race between async INSERT into Distributed and DETACH.
# Before the fix, this could cause "Cannot schedule a file" LOGICAL_ERROR.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nmq "
    DROP TABLE IF EXISTS dist_03998;
    DROP TABLE IF EXISTS local_03998;
    CREATE TABLE local_03998 (x UInt64) ENGINE = Null;
    CREATE TABLE dist_03998 (x UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local_03998, x);
"

function insert_thread()
{
    for _ in {1..100}; do
        ${CLICKHOUSE_CURL} -sfS "$CLICKHOUSE_URL&distributed_foreground_insert=0&prefer_localhost_replica=0" -d "INSERT INTO dist_03998 VALUES (1)(2)" >& /dev/null
    done
}

function detach_attach_thread()
{
    # Small delay to do some INSERTs just in case
    sleep 0.3
    for _ in {1..100}; do
        ${CLICKHOUSE_CURL} -sfS "$CLICKHOUSE_URL" -d "DETACH TABLE dist_03998" >& /dev/null
        ${CLICKHOUSE_CURL} -sfS "$CLICKHOUSE_URL" -d "ATTACH TABLE dist_03998" >& /dev/null
    done
}

insert_thread &
detach_attach_thread &

wait

# Reattach in case the table was left detached
$CLICKHOUSE_CLIENT -q "ATTACH TABLE dist_03998" 2>/dev/null

# Flush any remaining async inserts
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH DISTRIBUTED dist_03998"

$CLICKHOUSE_CLIENT -nmq "
    DROP TABLE dist_03998;
    DROP TABLE local_03998;
"

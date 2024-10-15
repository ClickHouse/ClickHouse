#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS wikistat1 SYNC;
DROP TABLE IF EXISTS wikistat2 SYNC;
"

for i in {1..2}; do
    $CLICKHOUSE_CLIENT --query "
    CREATE TABLE wikistat$i
    (
        time DateTime,
        project LowCardinality(String),
        subproject LowCardinality(String),
        path String,
        hits UInt64,
        PROJECTION total
        (
            SELECT
                project,
                subproject,
                path,
                sum(hits),
                count()
            GROUP BY
                project,
                subproject,
                path
        )
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/02494_zero_copy_projection_cancel_fetch', '$i')
    ORDER BY (path, time)
    SETTINGS min_bytes_for_wide_part = 0, storage_policy = 's3_cache',
    allow_remote_fs_zero_copy_replication = 1,
    max_replicated_fetches_network_bandwidth = 100
    "
done

$CLICKHOUSE_CLIENT --query "SYSTEM STOP FETCHES wikistat2"
$CLICKHOUSE_CLIENT --query "INSERT INTO wikistat1 SELECT toDateTime('2020-10-01 00:00:00'), 'hello', 'world', '/data/path', 10 from numbers(1000)"

$CLICKHOUSE_CLIENT --query "SYSTEM START FETCHES wikistat2"
$CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA wikistat2" &

# With previous versions LOGICAL_ERROR will be thrown
# and server will be crashed in debug mode.
sleep 1.5
$CLICKHOUSE_CLIENT --query "SYSTEM STOP FETCHES wikistat2"
sleep 1.5

$CLICKHOUSE_CLIENT --query "ALTER TABLE wikistat2 MODIFY SETTING max_replicated_fetches_network_bandwidth = 0"
$CLICKHOUSE_CLIENT --query "SYSTEM START FETCHES wikistat2"
wait

$CLICKHOUSE_CLIENT --query "SELECT count() FROM wikistat1 WHERE NOT ignore(*)"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM wikistat2 WHERE NOT ignore(*)"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS wikistat1 SYNC;
DROP TABLE IF EXISTS wikistat2 SYNC;
"

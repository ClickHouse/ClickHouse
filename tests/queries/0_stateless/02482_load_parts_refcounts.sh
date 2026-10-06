#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree
# no-shared-merge-tree: SharedMergeTree doesn't load inactive parts to memory after restart

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS load_parts_refcounts SYNC;

    CREATE TABLE load_parts_refcounts (id UInt32)
    ENGINE = ReplicatedMergeTree('/test/02482_load_parts_refcounts/{database}/{table}', '1')
    ORDER BY id SETTINGS old_parts_lifetime=100500;

    SYSTEM STOP MERGES load_parts_refcounts;

    INSERT INTO load_parts_refcounts VALUES (1);
    INSERT INTO load_parts_refcounts VALUES (2);
    INSERT INTO load_parts_refcounts VALUES (3);

    SYSTEM START MERGES load_parts_refcounts;
"

query_with_retry "OPTIMIZE TABLE load_parts_refcounts FINAL SETTINGS optimize_throw_if_noop = 1"

$CLICKHOUSE_CLIENT --query "DETACH TABLE load_parts_refcounts"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE load_parts_refcounts"

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT LOADING PARTS load_parts_refcounts"

# Background threads (e.g. the cleanup thread clearing caches of outdated parts) can hold a part for a moment,
# so wait until only the table references the reloaded outdated parts.
for _ in {1..60}; do
    refcounts=$($CLICKHOUSE_CLIENT --query "
        SELECT DISTINCT refcount FROM system.parts
        WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'load_parts_refcounts' AND NOT active")
    [[ "$refcounts" == "1" ]] && break
    sleep 0.5
done
echo "$refcounts"

$CLICKHOUSE_CLIENT --query "DROP TABLE load_parts_refcounts SYNC"

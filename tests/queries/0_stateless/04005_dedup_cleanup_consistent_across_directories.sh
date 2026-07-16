#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-fasttest, no-shared-merge-tree, no-async-insert
# Tag no-replicated-database: Requires investigation
# no-shared-merge-tree: relies on zookeeper structure of rmt
# Tag no-async-insert: relies on synchronous inserts

# Regression test for inconsistent deduplication block cleanup across
# blocks/ and deduplication_hashes/ ZooKeeper directories.
#
# With COMPATIBLE_DOUBLE_HASHES (default), each insert creates entries in both
# directories with different node names (SYNC hash vs UNIFIED hash). If cleanup
# removes different logical inserts from each directory, the union of remaining
# entries can cover all original hashes, causing re-inserts to be incorrectly
# deduplicated.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_TEST_ZOOKEEPER_PREFIX="${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS dedup_cleanup;"

$CLICKHOUSE_CLIENT --query="
CREATE TABLE dedup_cleanup (
    date Date,
    id UInt32,
    value String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dedup_cleanup/{shard}', '{replica}')
PARTITION BY date
ORDER BY (id)
SETTINGS replicated_deduplication_window = 2, cleanup_delay_period=4, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;"

# Insert 3 rows (each in a separate insert to create separate dedup entries)
$CLICKHOUSE_CLIENT --query="INSERT INTO dedup_cleanup VALUES (toDate('2024-01-01'), 1, 'a')"
$CLICKHOUSE_CLIENT --query="INSERT INTO dedup_cleanup VALUES (toDate('2024-01-01'), 2, 'b')"
$CLICKHOUSE_CLIENT --query="INSERT INTO dedup_cleanup VALUES (toDate('2024-01-01'), 3, 'c')"

$CLICKHOUSE_CLIENT --query="SELECT count(*) from dedup_cleanup" # 3

# Get the resolved ZK table path (with macros expanded) to query both directories.
zk_path=$($CLICKHOUSE_CLIENT --query="SELECT replica_path FROM system.replicas WHERE database = currentDatabase() AND table = 'dedup_cleanup'" | sed 's|/replicas/.*||')

wait_for_cleanup() {
    local dir=$1
    local expected=$2
    local count
    count=$($CLICKHOUSE_CLIENT --query="SELECT COUNT(*) FROM system.zookeeper WHERE path = '$zk_path/$dir'")
    local i=0
    while [[ $count != "$expected" ]] && [[ $i -lt 60 ]]; do
        sleep 1
        count=$($CLICKHOUSE_CLIENT --query="SELECT COUNT(*) FROM system.zookeeper WHERE path = '$zk_path/$dir'")
        i=$((i + 1))
    done
    if [[ $count != "$expected" ]]; then
        echo "Timeout waiting for $dir to reach $expected entries (got $count)" >&2
        return 1
    fi
}

# Wait for BOTH directories to be cleaned up to window size
wait_for_cleanup "blocks" 2
wait_for_cleanup "deduplication_hashes" 2

# Re-insert the first row. Since its entry was cleaned up from both directories,
# it should NOT be deduplicated.
$CLICKHOUSE_CLIENT --query="INSERT INTO dedup_cleanup VALUES (toDate('2024-01-01'), 1, 'a')"

$CLICKHOUSE_CLIENT --query="SELECT count(*) from dedup_cleanup" # 4

# Wait for cleanup again
wait_for_cleanup "blocks" 2
wait_for_cleanup "deduplication_hashes" 2

# Re-insert the second row
$CLICKHOUSE_CLIENT --query="INSERT INTO dedup_cleanup VALUES (toDate('2024-01-01'), 2, 'b')"

$CLICKHOUSE_CLIENT --query="SELECT count(*) from dedup_cleanup" # 5

# Wait for cleanup again
wait_for_cleanup "blocks" 2
wait_for_cleanup "deduplication_hashes" 2

# Re-insert the same row again without waiting for cleanup - this SHOULD be deduplicated
$CLICKHOUSE_CLIENT --query="INSERT INTO dedup_cleanup VALUES (toDate('2024-01-01'), 2, 'b')"

$CLICKHOUSE_CLIENT --query="SELECT count(*) from dedup_cleanup" # still 5

$CLICKHOUSE_CLIENT -q "DROP TABLE dedup_cleanup"

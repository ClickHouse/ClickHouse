#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-fasttest, no-shared-merge-tree, no-async-insert
# Tag no-replicated-database: Requires investigation
# no-shared-merge-tree: relies on zookeeper structure of rmt
# Tag no-async-insert: relies on synchronous inserts, can be ajusted to work with async inserts but not worth the effort

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_TEST_ZOOKEEPER_PREFIX="${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS elog;"

$CLICKHOUSE_CLIENT --query="
CREATE TABLE elog (
    date Date,
    engine_id UInt32,
    referrer String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/elog/{shard}', '{replica}')
PARTITION BY date
ORDER BY (engine_id)
SETTINGS replicated_deduplication_window = 2, cleanup_delay_period=4, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;"

$CLICKHOUSE_CLIENT --query="INSERT INTO elog VALUES (toDate('2018-10-01'), 1, 'hello')"
$CLICKHOUSE_CLIENT --query="INSERT INTO elog VALUES (toDate('2018-10-01'), 2, 'hello')"
$CLICKHOUSE_CLIENT --query="INSERT INTO elog VALUES (toDate('2018-10-01'), 3, 'hello')"

$CLICKHOUSE_CLIENT --query="SELECT count(*) from elog" # 3 rows

# Get the resolved ZK table path (with macros expanded) to query both directories.
zk_path=$($CLICKHOUSE_CLIENT --query="SELECT replica_path FROM system.replicas WHERE database = currentDatabase() AND table = 'elog'" | sed 's|/replicas/.*||')

# Wait for BOTH blocks/ and deduplication_hashes/ directories to be cleaned up to window size.
# With COMPATIBLE_DOUBLE_HASHES (default), each insert creates entries in both directories.
# We must wait for both to be cleaned, because the cleanup thread processes them sequentially
# and an insert between the two cleanups can cause them to have different entry counts.
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

wait_for_cleanup "blocks" 2
wait_for_cleanup "deduplication_hashes" 2

$CLICKHOUSE_CLIENT --query="INSERT INTO elog VALUES (toDate('2018-10-01'), 1, 'hello')"

$CLICKHOUSE_CLIENT --query="SELECT count(*) from elog" # 4 rows

wait_for_cleanup "blocks" 2
wait_for_cleanup "deduplication_hashes" 2

$CLICKHOUSE_CLIENT --query="INSERT INTO elog VALUES (toDate('2018-10-01'), 2, 'hello')"

$CLICKHOUSE_CLIENT --query="SELECT count(*) from elog" # 5 rows

wait_for_cleanup "blocks" 2
wait_for_cleanup "deduplication_hashes" 2

$CLICKHOUSE_CLIENT --query="INSERT INTO elog VALUES (toDate('2018-10-01'), 2, 'hello')"

$CLICKHOUSE_CLIENT --query="SELECT count(*) from elog" # still 5 rows

$CLICKHOUSE_CLIENT -q "DROP TABLE elog"

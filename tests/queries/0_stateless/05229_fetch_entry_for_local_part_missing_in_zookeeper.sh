#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree
# no-parallel: the log entry below is written into ZooKeeper by hand, which needs the table's queue
#   to itself while it is processed.
# no-shared-merge-tree: the part's node under `replicas/<r>/parts/` that the test removes by hand
#   is a `ReplicatedMergeTree` structure, and the entry it then watches is a `ReplicatedMergeTree`
#   queue entry; neither exists when the engine is substituted.

# A part that is active locally while its node under `replicas/<r>/parts/` is gone - a state crash
# recovery can leave behind - wedged the replication queue: the `GET_PART` entry for it fetched the
# whole part from a peer and then threw `DUPLICATE_DATA_PART` for the part that is already there, and
# nothing in the retry path reconciled the two, so the entry was retried forever. The part check
# thread is the reconciler, so the entry has to wait for it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

zk_path="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_fetch_missing_znode"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_fetch_missing_znode_r1 SYNC;
DROP TABLE IF EXISTS t_fetch_missing_znode_r2 SYNC;

CREATE TABLE t_fetch_missing_znode_r1 (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_fetch_missing_znode', 'r1') ORDER BY id;

CREATE TABLE t_fetch_missing_znode_r2 (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_fetch_missing_znode', 'r2') ORDER BY id;

INSERT INTO t_fetch_missing_znode_r1 SELECT number, number FROM numbers(100);
SYSTEM SYNC REPLICA t_fetch_missing_znode_r2;
"

part_name=$(${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 't_fetch_missing_znode_r2' AND active")

# The self-inconsistent state: the part stays in the working set, its node in ZooKeeper is gone.
${CLICKHOUSE_KEEPER_CLIENT} -q "rm '$zk_path/replicas/r2/parts/$part_name'"
echo -n 'the node of the local part is gone: '
${CLICKHOUSE_KEEPER_CLIENT} -q "ls '$zk_path/replicas/r2/parts'" | grep -c "$part_name"

# A fetch entry for the part the replica already holds, written by hand because this state is not
# reachable through SQL.
part_type=$(${CLICKHOUSE_CLIENT} -q "
    SELECT part_type FROM system.parts
    WHERE database = currentDatabase() AND table = 't_fetch_missing_znode_r2' AND active")

${CLICKHOUSE_KEEPER_CLIENT} -q "create '$zk_path/log/log-' 'format version: 4
create_time: 2026-01-01 00:00:00
source replica: r1
block_id: 
get
$part_name
part_type: $part_type
' PERSISTENT SEQUENTIAL"

# The entry waits: it names the state instead of fetching the part and throwing
# `DUPLICATE_DATA_PART` for the copy that is already there. (The part check thread, which the entry
# waits for, leaves a part younger than five minutes alone, so this does not wait for the whole
# reconciliation - what matters here is that the entry stops re-downloading the part.)
for _ in {1..120}
do
    last_exception=$(${CLICKHOUSE_CLIENT} -q "
        SELECT last_exception FROM system.replication_queue
        WHERE database = currentDatabase() AND table = 't_fetch_missing_znode_r2'")
    if [[ -n "$last_exception" ]]; then
        break
    fi
    sleep 0.5
done

echo -n 'the entry names the state instead of fetching: '
echo "$last_exception" | grep -c -m1 'has no node in ZooKeeper'
echo -n 'and it does not report a duplicate part: '
echo "$last_exception" | grep -c 'DUPLICATE_DATA_PART'

${CLICKHOUSE_CLIENT} -q "
SELECT 'the part is still there', count(), sum(v) FROM t_fetch_missing_znode_r2;
SELECT 'and still active', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_fetch_missing_znode_r2' AND active;

DROP TABLE t_fetch_missing_znode_r2 SYNC;
DROP TABLE t_fetch_missing_znode_r1 SYNC;
"

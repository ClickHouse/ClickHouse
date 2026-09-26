#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree
# no-parallel: the log entry below is written into ZooKeeper by hand, which needs the table's queue
#   to itself while it is processed.
# no-shared-merge-tree: the part's node under `replicas/<r>/parts/` that the test removes by hand
#   is a `ReplicatedMergeTree` structure, and the entry it then watches is a `ReplicatedMergeTree`
#   queue entry; neither exists when the engine is substituted.

# The companion of `05229_fetch_entry_for_local_part_missing_in_zookeeper` for the merge and mutation
# entries, which never reach `executeLogEntry`: `MergeFromLogEntryTask` and `MutateFromLogEntryTask`
# check the same "the part is here but its node is not" state in
# `ReplicatedMergeMutateTaskBase::checkExistingPart`. The entry has to wait for the part check thread
# there as well instead of producing the part again and throwing `DUPLICATE_DATA_PART` for the copy
# that is already there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

zk_path="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_merge_missing_znode"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_merge_missing_znode_r1 SYNC;
DROP TABLE IF EXISTS t_merge_missing_znode_r2 SYNC;

CREATE TABLE t_merge_missing_znode_r1 (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_merge_missing_znode', 'r1') ORDER BY id;

CREATE TABLE t_merge_missing_znode_r2 (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_merge_missing_znode', 'r2') ORDER BY id;

INSERT INTO t_merge_missing_znode_r1 SELECT number, number FROM numbers(50);
INSERT INTO t_merge_missing_znode_r1 SELECT number + 50, number + 50 FROM numbers(50);
OPTIMIZE TABLE t_merge_missing_znode_r1 FINAL;
SYSTEM SYNC REPLICA t_merge_missing_znode_r2;
"

part_name=$(${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 't_merge_missing_znode_r2' AND active")

# `MergeFromLogEntryTask` names its logger after the table's UUID, not after `<database>.<table>`.
table_uuid=$(${CLICKHOUSE_CLIENT} -q "
    SELECT uuid FROM system.tables
    WHERE database = currentDatabase() AND name = 't_merge_missing_znode_r2'")

# The self-inconsistent state: the merged part stays in the working set, its node in ZooKeeper is gone.
${CLICKHOUSE_KEEPER_CLIENT} -q "rm '$zk_path/replicas/r2/parts/$part_name'"
echo -n 'the node of the local part is gone: '
${CLICKHOUSE_KEEPER_CLIENT} -q "ls '$zk_path/replicas/r2/parts'" | grep -c "$part_name"

# A merge entry producing the part the replica already holds, written by hand because this state is
# not reachable through SQL.
${CLICKHOUSE_KEEPER_CLIENT} -q "create '$zk_path/log/log-' 'format version: 4
create_time: 2026-01-01 00:00:00
source replica: r1
block_id: 
merge
all_0_0_0
all_1_1_0
into
$part_name
deduplicate: 0
' PERSISTENT SEQUENTIAL"

# The task names the state and waits for the part check thread instead of merging or fetching the
# part again. (The part check thread leaves a part younger than five minutes alone, so this does not
# wait for the whole reconciliation - what matters here is that the entry stops producing the part.)
found=0
for _ in {1..120}
do
    found=$(${CLICKHOUSE_CLIENT} -q "
        SYSTEM FLUSH LOGS text_log;
        SELECT count() FROM system.text_log
        WHERE logger_name = '${table_uuid}::${part_name} (MergeFromLogEntryTask)'
          AND message LIKE '%exists locally but has no node in ZooKeeper%'")
    if [[ "$found" != "0" ]]; then
        break
    fi
    sleep 0.5
done

echo -n 'the merge entry names the state instead of producing the part: '
[[ "$found" != "0" ]] && echo 1 || echo 0

echo -n 'and it does not report a duplicate part: '
${CLICKHOUSE_CLIENT} -q "
    SELECT countIf(last_exception LIKE '%DUPLICATE_DATA_PART%') FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 't_merge_missing_znode_r2'"

${CLICKHOUSE_CLIENT} -q "
SELECT 'the part is still there', count(), sum(v) FROM t_merge_missing_znode_r2;
SELECT 'and still active', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_merge_missing_znode_r2' AND active;

DROP TABLE t_merge_missing_znode_r2 SYNC;
DROP TABLE t_merge_missing_znode_r1 SYNC;
"

#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database, no-ordinary-database
# A replicated table attached while its Keeper path holds nothing stays read-only instead of failing
# to attach. Reading the part moves of such a table must report that it has none, not an error.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A full-definition ATTACH warns on stderr, which says nothing about the reads under test.
CLIENT="${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null"

TABLE_UUID=$(${CLIENT} -q "SELECT generateUUIDv4()")
${CLIENT} -q "ATTACH TABLE no_keeper_tree UUID '${TABLE_UUID}' (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/absent', 'r1') ORDER BY c0" 2>/dev/null

# In range: the table really is attached and really is read-only, which is the state under test.
${CLIENT} -q "SELECT count(), max(is_readonly) FROM system.replicas
              WHERE database = currentDatabase() AND table = 'no_keeper_tree'"

# The two reads that used to fail. Both are scoped to this database, so no other test's table can
# supply the answer and no other test can be disturbed.
${CLIENT} -q "SELECT count() FROM system.part_moves_between_shards WHERE database = currentDatabase()"
${CLIENT} -q "KILL PART_MOVE_TO_SHARD WHERE database = currentDatabase()
              AND task_uuid = '00000000-0000-0000-0000-000000000000'"

# The reads above cannot tell a tolerated missing node from a read that never happened, so assert the
# listing really was attempted on the absent path and really was answered with no such node.
${CLIENT} -q "SYSTEM FLUSH LOGS zookeeper_log"
${CLIENT} -q "SELECT count() > 0 FROM system.zookeeper_log
              WHERE path = '/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/absent/part_moves_shard'
              AND type = 'Response' AND error = 'ZNONODE'"

${CLIENT} -q "DROP TABLE no_keeper_tree SYNC"

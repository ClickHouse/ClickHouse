#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree
# no-parallel: the pause failpoints affect background mutations and task removal server-wide.
# no-shared-merge-tree: this tests the plain `MergeTree` read-only transition.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

merge_pause=mt_mutate_task_pause_in_prepare
drain_pause=merge_tree_background_task_marked_for_deletion

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT $drain_pause"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT $merge_pause"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS readonly_drain SYNC;
CREATE TABLE readonly_drain (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO readonly_drain VALUES (1), (2);
SYSTEM ENABLE FAILPOINT $merge_pause;
SYSTEM ENABLE FAILPOINT $drain_pause;
ALTER TABLE readonly_drain UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
"

# Hold an active mutation before it writes its result part.
timeout 30s ${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT $merge_pause PAUSE"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE readonly_drain MODIFY SETTING table_readonly = 1" &
alter_pid=$!

# The transition must reach the executor's cancellation/drain path. Before the fix,
# the ALTER returns without visiting this failpoint and this wait times out.
timeout 30s ${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT $drain_pause PAUSE"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT $drain_pause"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT $merge_pause"
wait "$alter_pid"

${CLICKHOUSE_CLIENT} --multiquery --query "
SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 'readonly_drain';
SELECT sum(x) FROM readonly_drain;
ALTER TABLE readonly_drain MODIFY SETTING table_readonly = 0;
ALTER TABLE readonly_drain UPDATE x = x + 10 WHERE 1 SETTINGS mutations_sync = 2;
SELECT sum(x) FROM readonly_drain;
DROP TABLE readonly_drain SYNC;
"

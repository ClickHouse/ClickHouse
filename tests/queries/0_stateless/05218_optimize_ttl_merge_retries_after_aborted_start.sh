#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# - no-parallel: the test toggles the server-global failpoints
#   `mt_fail_selected_merge_before_start_once` and `mt_merge_selecting_task_pause_when_scheduled`.
# - no-fasttest: failpoints are not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Selecting a `TTLDelete` merge postpones the next TTL merge of the partition by
# `merge_with_ttl_timeout`. If the selected merge then dies before it starts - the future part
# cannot be constructed, or the parts cannot be tagged and the disk space reserved - that
# postponement has to be given back: it is shared with background selection, and a partially expired
# single part has no regular merge to fall back to, so a leaked postponement would defer its TTL
# rewrite for the whole timeout. Here the failure is injected right where the parts are tagged; the
# next `OPTIMIZE` must then select the TTL merge again and drop the expired row.

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ttl_aborted_start SYNC"

# A single part with one expired and one live row, so its only possible merge is a `TTLDelete`
# rewrite. `ttl_only_drop_parts = 0` keeps it a rewrite instead of a part drop (a `TTLDrop` merge
# postpones nothing), and the long `merge_with_ttl_timeout` makes a leaked postponement fatal for the
# test instead of merely slow. TTL merges stay stopped until everything else is in place, so that
# nothing can rewrite the part ahead of the `OPTIMIZE` below.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_ttl_aborted_start (k UInt64, d DateTime)
    ENGINE = MergeTree ORDER BY k
    TTL d + INTERVAL 1 SECOND
    SETTINGS optimize_on_insert = 0, ttl_only_drop_parts = 0, merge_with_ttl_timeout = 10000"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP TTL MERGES t_ttl_aborted_start"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_ttl_aborted_start VALUES (1, now() - INTERVAL 1 DAY), (2, now() + INTERVAL 1 DAY)"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_fail_selected_merge_before_start_once" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ttl_aborted_start SYNC" 2>/dev/null
}
trap cleanup EXIT

# Park background merge selection, so that the TTL rewrite below can only be performed by the
# foreground `OPTIMIZE` - a background selection would hide a leaked postponement by doing the
# rewrite itself. Foreground `OPTIMIZE` does not go through `scheduleDataProcessingJob`.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT mt_merge_selecting_task_pause_when_scheduled PAUSE"

$CLICKHOUSE_CLIENT --query "SYSTEM START TTL MERGES t_ttl_aborted_start"

# The first `OPTIMIZE` selects the TTL merge and fails before starting it.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT mt_fail_selected_merge_before_start_once"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_ttl_aborted_start" 2>&1 | grep -m1 -o "FAULT_INJECTED"
$CLICKHOUSE_CLIENT --query "SELECT 'rows after failed optimize', count() FROM t_ttl_aborted_start"

# The second one must select the TTL merge again and drop the expired row.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_ttl_aborted_start"
$CLICKHOUSE_CLIENT --query "SELECT 'rows after optimize', count(), min(k) FROM t_ttl_aborted_start"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_merge_selecting_task_pause_when_scheduled"

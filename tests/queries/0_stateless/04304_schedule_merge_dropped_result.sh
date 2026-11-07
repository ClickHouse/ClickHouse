#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression for a scheduled-merge result that is produced (by the background merge) and then
# dropped without ever running SYSTEM SYNC MERGES. The merge definition is still remembered, but
# its inputs have been consumed, so the dropped result can never reappear: re-scheduling a merge
# that references it must be rejected rather than left to hang SYNC MERGES forever.

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_manual_dropped"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_manual_dropped (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS merge_selector_algorithm = 'Manual'"

$CLICKHOUSE_CLIENT --query "INSERT INTO t_manual_dropped VALUES (1)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_manual_dropped VALUES (2)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_manual_dropped VALUES (3)"

# Schedule the merge but do NOT call SYSTEM SYNC MERGES; let the background merge produce all_1_2_1.
$CLICKHOUSE_CLIENT --query "SYSTEM SCHEDULE MERGE t_manual_dropped PARTS 'all_1_1_0', 'all_2_2_0'"

produced=0
for _ in {1..60}; do
    produced=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_manual_dropped' AND active AND name = 'all_1_2_1'")
    [ "$produced" = "1" ] && break
    sleep 0.5
done

if [ "$produced" != "1" ]; then
    echo "all_1_2_1 was not produced within the timeout" >&2
    $CLICKHOUSE_CLIENT --query "DROP TABLE t_manual_dropped"
    exit 1
fi

$CLICKHOUSE_CLIENT --query "ALTER TABLE t_manual_dropped DETACH PART 'all_1_2_1'"

# all_1_2_1 is gone and its inputs are consumed, so the merge can never produce it again.
$CLICKHOUSE_CLIENT --query "SYSTEM SCHEDULE MERGE t_manual_dropped PARTS 'all_1_2_1', 'all_3_3_0'" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

$CLICKHOUSE_CLIENT --query "DROP TABLE t_manual_dropped"

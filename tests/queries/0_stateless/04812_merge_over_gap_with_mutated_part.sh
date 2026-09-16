#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-ordinary-database, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS table_with_mutated_gap SYNC;"

# max_bytes_to_merge_at_max_space_in_pool = 0 disables background merges while leaving explicit
# OPTIMIZE and mutations working; SYSTEM STOP MERGES would also stop mutation selection.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE table_with_mutated_gap (x UInt64, s String) ENGINE = MergeTree() ORDER BY x
    SETTINGS old_parts_lifetime = 10000, max_bytes_to_merge_at_max_space_in_pool = 0,
             cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
             max_cleanup_delay_period = 1;"

$CLICKHOUSE_CLIENT --query "INSERT INTO table_with_mutated_gap VALUES (1, 'left');"
$CLICKHOUSE_CLIENT --query "INSERT INTO table_with_mutated_gap VALUES (2, 'middle');"

# Deletes every row of the middle part and throws on the left one, so only the middle part
# advances its mutation version. A mutation that completes everywhere cannot build the gap:
# every active part would reach the same version.
$CLICKHOUSE_CLIENT --query "
    ALTER TABLE table_with_mutated_gap DELETE WHERE x = 2 OR throwIf(s = 'left', 'poison')
    SETTINGS mutations_sync = 0;"

# Per-part outcomes are published independently, so latest_fail_reason can become non-empty
# before the middle part's empty result is committed. Waiting for that part too is required:
# the KILL below cancels a still pending task, and a committed part can no longer be cancelled.
for _ in {1..120}; do
    failed=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.mutations
        WHERE table = 'table_with_mutated_gap' AND database = currentDatabase() AND latest_fail_reason != '';")
    committed=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.parts
        WHERE table = 'table_with_mutated_gap' AND database = currentDatabase() AND rows = 0;")
    [ "$failed" -eq 1 ] && [ "$committed" -ge 1 ] && break
    sleep 0.5
done
$CLICKHOUSE_CLIENT --query "SELECT 'mutation failed on one part', $failed == 1;"

# Leaves the left part at its lower version and lets merge selection consider the range again.
$CLICKHOUSE_CLIENT --query "
    KILL MUTATION WHERE table = 'table_with_mutated_gap' AND database = currentDatabase() SYNC FORMAT Null;"

# The empty mutation result is a gap only once it is Outdated: the collector takes active parts
# only. clearEmptyParts does that in a background task.
for _ in {1..120}; do
    outdated=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.parts
        WHERE table = 'table_with_mutated_gap' AND database = currentDatabase() AND rows = 0 AND active = 0;")
    [ "$outdated" -eq 1 ] && break
    sleep 0.5
done
$CLICKHOUSE_CLIENT --query "SELECT 'gap part is outdated', $outdated == 1;"

# Inserted after the kill, so it carries mutation version 0.
$CLICKHOUSE_CLIENT --query "INSERT INTO table_with_mutated_gap VALUES (3, 'right');"

$CLICKHOUSE_CLIENT --query "SELECT 'parts with gap';"
$CLICKHOUSE_CLIENT --query "
    SELECT name, rows, active, level FROM system.parts
    WHERE table = 'table_with_mutated_gap' AND database = currentDatabase()
    ORDER BY min_block_number, max_block_number, level, name;"

# Asserts the shape the merge below must refuse: the gap part's mutation version strictly
# exceeds both neighbours' while all levels are equal.
$CLICKHOUSE_CLIENT --query "
    SELECT 'gap mutation exceeds neighbours, levels equal',
           max(if(rows = 0, mutation, 0)) > max(if(rows = 0, 0, mutation)),
           uniqExact(level) == 1
    FROM (
        SELECT rows, level, toInt64(splitByChar('_', name)[-1]) AS mutation
        FROM system.parts
        WHERE table = 'table_with_mutated_gap' AND database = currentDatabase()
          AND (active OR rows = 0)
    );"

$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE table_with_mutated_gap FINAL SETTINGS optimize_throw_if_noop = 1;" 2>&1 |\
  grep "There is an outdated part in a gap between two active parts" |\
  grep -o "CANNOT_ASSIGN_OPTIMIZE" | uniq || echo "NO ERROR"

# Loading the parts is what rejects an intersecting pair, so the refusal above is only
# meaningful together with a reattach.
$CLICKHOUSE_CLIENT --query "DETACH TABLE table_with_mutated_gap;"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE table_with_mutated_gap;"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT LOADING PARTS table_with_mutated_gap;"

$CLICKHOUSE_CLIENT --query "SELECT 'rows after detach/attach', count() FROM table_with_mutated_gap;"

$CLICKHOUSE_CLIENT --query "DROP TABLE table_with_mutated_gap SYNC;"

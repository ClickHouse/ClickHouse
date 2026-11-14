#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY: a 0-row DELETE marker holding the only copy of another part's kills is never reclaimed.
# no-parallel: an outdated part is removed only once every running transaction on the server started
# after its removal, so another test's transaction holds the wait back.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./parts.lib
. "$CURDIR"/parts.lib

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1"

CLEANUP_SETTINGS="remove_empty_parts = 1,
         old_parts_lifetime = 0,
         cleanup_delay_period = 1,
         max_cleanup_delay_period = 1,
         cleanup_delay_period_random_add = 0"

# Red if the marker stops pinning itself against empty-part cleanup (`marker_still_active` 0) and
# old-part removal (`survivors_after_cleanup` has all ten rows).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_pin SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_pin_control SYNC"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE uk_pin (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS $CLEANUP_SETTINGS"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE uk_pin_control (id UInt64)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS $CLEANUP_SETTINGS"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES uk_pin"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_pin SELECT number, 'a' FROM numbers(10)"
$CLICKHOUSE_CLIENT --query "DELETE FROM uk_pin WHERE id % 2 = 0"

$CLICKHOUSE_CLIENT --query "
    SELECT 'held_on_marker', unique_key_bitmap_versions FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_pin' AND active AND rows = 0"

# Control: a 0-row part with nothing to hold; its reclaim shows the dropper ran.
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_pin_control SELECT number FROM numbers(10)"
$CLICKHOUSE_CLIENT --query "ALTER TABLE uk_pin_control DELETE WHERE 1 SETTINGS mutations_sync = 2"

reclaimed=0
wait_for_delete_empty_parts uk_pin_control "$CLICKHOUSE_DATABASE" 120 \
    && wait_for_delete_inactive_parts uk_pin_control "$CLICKHOUSE_DATABASE" 120 && reclaimed=1
echo -e "control_empty_part_reclaimed\t$reclaimed"

$CLICKHOUSE_CLIENT --query "
    SELECT 'marker_still_active', count() = 1 FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_pin' AND active AND rows = 0"

$CLICKHOUSE_CLIENT --query "
    SELECT 'survivors_after_cleanup', groupArray(id) FROM (SELECT id FROM uk_pin ORDER BY id)"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_pin SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE uk_pin_control SYNC"

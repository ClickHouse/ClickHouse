#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY: a background merge that retires bitmap holders without their target carries the kills.
#   1. one version: merging the DELETE marker away carries its kills and frees the marker
#   2. three versions: a merge of three holders carries one file per version and frees all three
# The manual merge selector is what picks the holders without the target.
# no-parallel: an outdated part is removed only once every running transaction on the server started
# after its removal, so another test's transaction holds the wait back.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./parts.lib
. "$CURDIR"/parts.lib

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1"

# The name of table `$1`'s one active part matching `$2`.
part_where() {
    $CLICKHOUSE_CLIENT --query "
        SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$1' AND active AND $2"
}

CLEANUP_SETTINGS="merge_selector_algorithm = 'Manual',
         min_bytes_for_wide_part = 0,
         old_parts_lifetime = 0,
         cleanup_delay_period = 1,
         max_cleanup_delay_period = 1,
         cleanup_delay_period_random_add = 0"

# 1. one version: red if a carried bitmap does not release the source that held it, or the carry
# itself is dropped (`marker_reclaimed` 0; the rows stay dead either way).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_carry SYNC"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE uk_carry (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS $CLEANUP_SETTINGS"

# The first part is the target; the second gives the merge a second source.
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_carry SELECT number, 'a' FROM numbers(0, 10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_carry SELECT number, 'b' FROM numbers(100, 10)"
$CLICKHOUSE_CLIENT --query "DELETE FROM uk_carry WHERE id < 5"

target=$(part_where uk_carry "min_block_number = (SELECT min(min_block_number) FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_carry' AND active)")
source=$(part_where uk_carry "rows > 0 AND name != '$target'")
marker=$(part_where uk_carry "rows = 0")

$CLICKHOUSE_CLIENT --query "
    SELECT 'held_by_the_marker', arrayMap(x -> replaceAll(x, '$target', 'target'), unique_key_bitmap_versions)
    FROM system.parts WHERE database = currentDatabase() AND table = 'uk_carry' AND active AND name = '$marker'"

# The marker and one data part, NOT the target.
$CLICKHOUSE_CLIENT --max_execution_time 60 --query "
    SYSTEM SCHEDULE MERGE uk_carry PARTS '$source', '$marker'"
$CLICKHOUSE_CLIENT --max_execution_time 60 --query "SYSTEM SYNC MERGES uk_carry"

$CLICKHOUSE_CLIENT --query "
    SELECT 'carried_into_the_result',
           arrayMap(x -> replaceAll(replaceRegexpOne(x, '^[0-9]+_for_', 'csn_for_'), '$target', 'target'), unique_key_bitmap_versions)
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_carry' AND active AND rows > 0 AND name != '$target'"

$CLICKHOUSE_CLIENT --query "
    SELECT 'target_untouched', unique_key_bitmap_versions FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_carry' AND active AND name = '$target'"

reclaimed=0
wait_for_delete_empty_parts uk_carry "$CLICKHOUSE_DATABASE" 120 \
    && wait_for_delete_inactive_parts uk_carry "$CLICKHOUSE_DATABASE" 120 && reclaimed=1
echo -e "marker_reclaimed\t$reclaimed"

$CLICKHOUSE_CLIENT --query "
    SELECT 'survivors', groupArray(id) FROM (SELECT id FROM uk_carry ORDER BY id)"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_carry SYNC"

# 2. three versions: red if a merge folds a target's carried versions into one file
# (`pinned_sources` 2).
PINNED_STATE='Holds the delete bitmap of another part that is still in the part set'

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_unpin SYNC"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE uk_unpin (k UInt64, v UInt64)
ENGINE = MergeTree
UNIQUE KEY (k)
ORDER BY (k)
SETTINGS $CLEANUP_SETTINGS"

# The first part is the target; the next three each overwrite a different pair of its keys.
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_unpin SELECT number, 0 FROM numbers(6)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_unpin SELECT number, 1 FROM numbers(0, 2)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_unpin SELECT number, 2 FROM numbers(2, 2)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_unpin SELECT number, 3 FROM numbers(4, 2)"

target=$(part_where uk_unpin "rows = 6")
holders=$($CLICKHOUSE_CLIENT --query "
    SELECT arrayStringConcat(groupArray(concat('''', name, '''')), ', ') FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_unpin' AND active AND name != '$target' FORMAT TSVRaw")

echo "versions_of_the_target $($CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_unpin' AND active
      AND arrayExists(x -> x LIKE '%for_$target', unique_key_bitmap_versions)")"

# The three holders, NOT the target: a merge that took it would absorb the kills instead.
$CLICKHOUSE_CLIENT --max_execution_time 60 --query "SYSTEM SCHEDULE MERGE uk_unpin PARTS $holders"
$CLICKHOUSE_CLIENT --max_execution_time 60 --query "SYSTEM SYNC MERGES uk_unpin"

echo "carried_versions $($CLICKHOUSE_CLIENT --query "
    SELECT length(unique_key_bitmap_versions) FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_unpin' AND active AND name != '$target'")"

wait_for_delete_inactive_parts uk_unpin "$CLICKHOUSE_DATABASE" 30

echo "pinned_sources $($CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_unpin' AND removal_state = '${PINNED_STATE}'")"

$CLICKHOUSE_CLIENT --query "SELECT 'rows', count(), sum(v) FROM uk_unpin"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_unpin SYNC"

#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `TTLDrop` merge is assigned from the bounds stored in `ttl.txt`, so it must still evaluate the
# current TTL expression over the rows before deciding that nothing survives. `OPTIMIZE ... FINAL`
# always assigns `MergeType::Regular`, so these cases rely on background TTL merges.

function wait_for_ttl_merge_matching()
{
    local table=$1
    local reason_pattern=$2
    for _ in $(seq 1 300); do
        ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"
        local count
        count=$(${CLICKHOUSE_CLIENT} -q "
            SELECT count() FROM system.part_log
            WHERE database = currentDatabase() AND table = '$table'
              AND event_type = 'MergeParts' AND merge_reason LIKE '$reason_pattern'")
        if [ "$count" -gt "0" ]; then
            return
        fi
        sleep 0.1
    done
}

function wait_for_ttl_drop_merge()
{
    wait_for_ttl_merge_matching "$1" "TTLDropMerge"
}

function wait_for_row_count()
{
    local table=$1
    local want=$2
    for _ in $(seq 1 120); do
        local count
        count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM $table")
        if [ "$count" = "$want" ]; then
            return
        fi
        sleep 0.5
    done
}

echo "-- a TTLDrop merge keeps rows the current expression does not expire"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_drop_reeval (id UInt64, ts DateTime)
    ENGINE = MergeTree ORDER BY id
    TTL ts + INTERVAL 1 SECOND
    SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_ttl_drop_reeval;
    INSERT INTO t_ttl_drop_reeval SELECT number, now() - INTERVAL 1 DAY FROM numbers(100);
    ALTER TABLE t_ttl_drop_reeval MODIFY TTL ts + INTERVAL 10 YEAR SETTINGS materialize_ttl_after_modify = 0;
    SYSTEM START MERGES t_ttl_drop_reeval;
"

wait_for_ttl_drop_merge "t_ttl_drop_reeval"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_reeval"
# The merge has to open the source part to reach that verdict.
${CLICKHOUSE_CLIENT} -q "
    SELECT read_rows > 0 FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_drop_reeval'
      AND event_type = 'MergeParts' AND merge_reason = 'TTLDropMerge'
    ORDER BY event_time DESC LIMIT 1"
# The merge also rewrites the stored bounds, so the part is no longer a trap for the next merge.
${CLICKHOUSE_CLIENT} -q "
    SELECT delete_ttl_info_min > now() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_drop_reeval' AND active"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_reeval"

echo "-- a TTLDrop range honours the byte budget"

# Since the output is no longer known to be empty, the range is sized like any other merge. With a
# budget below the combined size of the expired parts, no single range can cover all of them.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_drop_sized (id UInt64, payload String, ts DateTime)
    ENGINE = MergeTree ORDER BY id
    TTL ts + INTERVAL 1 SECOND
    SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 0,
             max_bytes_to_merge_at_max_space_in_pool = 4096;

    SYSTEM STOP MERGES t_ttl_drop_sized;
    INSERT INTO t_ttl_drop_sized SELECT number, randomString(1000), now() - INTERVAL 1 DAY FROM numbers(20);
    INSERT INTO t_ttl_drop_sized SELECT number + 100, randomString(1000), now() - INTERVAL 1 DAY FROM numbers(20);
    INSERT INTO t_ttl_drop_sized SELECT number + 200, randomString(1000), now() - INTERVAL 1 DAY FROM numbers(20);
    SYSTEM START MERGES t_ttl_drop_sized;
"

wait_for_ttl_drop_merge "t_ttl_drop_sized"

${CLICKHOUSE_CLIENT} -q "
    SELECT max(length(merged_from)) = 1 FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_drop_sized'
      AND event_type = 'MergeParts' AND merge_reason = 'TTLDropMerge'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_sized"

echo "-- an expired part larger than the byte budget is still dropped"

# Without the single-part retry a part above the budget is refused by every range, so expired data
# on a full disk could never be freed.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_drop_oversized (id UInt64, payload String, ts DateTime)
    ENGINE = MergeTree ORDER BY id
    TTL ts + INTERVAL 1 SECOND
    SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 0,
             max_bytes_to_merge_at_max_space_in_pool = 1024;

    SYSTEM STOP MERGES t_ttl_drop_oversized;
    INSERT INTO t_ttl_drop_oversized SELECT number, randomString(1000), now() - INTERVAL 1 DAY FROM numbers(50);
    SYSTEM START MERGES t_ttl_drop_oversized;
"

wait_for_ttl_drop_merge "t_ttl_drop_oversized"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_oversized"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_oversized"

echo "-- a GROUP BY TTL whose rows survive stays selectable for a later rollup"

# A rule marked finished sets `part_min_ttl` to 0 and clears `has_any_non_finished_ttls`, so the
# part is never selected for a TTL merge again. No system table exposes the flag, so it is observed
# through that consequence: the rollup below can only run if the merge left the rule unfinished.
#
# The interval is part of the map key of a GROUP BY rule, so changing it would create a fresh key
# and recalculate from scratch. Patching the column the expression reads keeps the key, which is
# what leaves the stored bounds expired while no row is.
#
# `ts` lands 15 seconds ahead because the first merge has to run while no row is expired yet.
#
# The second merge has to be selector-driven. `OPTIMIZE ... FINAL` would reach the rows through
# `MergeTreeDataPartTTLInfos::update`, which clears this flag for GROUP BY rules, so a forced merge
# reports the same result whether or not the flag was set.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_group_by_stays (k UInt64, ts DateTime, v UInt64)
    ENGINE = MergeTree ORDER BY k
    TTL ts + INTERVAL 1 SECOND GROUP BY k SET v = max(v)
    SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1,
             enable_block_offset_column = 1, merge_with_ttl_timeout = 0;

    SYSTEM STOP MERGES t_ttl_group_by_stays;
    INSERT INTO t_ttl_group_by_stays VALUES (1, now() - INTERVAL 1 DAY, 10), (1, now() - INTERVAL 1 DAY, 20);
    SET enable_lightweight_update = 1;
    UPDATE t_ttl_group_by_stays SET ts = now() + INTERVAL 15 SECOND WHERE 1;
    SYSTEM START MERGES t_ttl_group_by_stays;
"

wait_for_ttl_merge_matching "t_ttl_group_by_stays" "TTL%"

# Both rows survived that merge, so the rollup below is a rule that was kept rather than a fixture
# that never had rows left to roll up.
${CLICKHOUSE_CLIENT} -q "
    SELECT rows FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_group_by_stays'
      AND event_type = 'MergeParts' AND merge_reason LIKE 'TTL%'
    ORDER BY event_time_microseconds LIMIT 1"

wait_for_row_count "t_ttl_group_by_stays" 1

${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(v) FROM t_ttl_group_by_stays"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_group_by_stays"

#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `TTLDrop` merge is assigned from the bounds stored in `ttl.txt`, so it must still evaluate the
# current TTL expression over the rows before deciding that nothing survives. `OPTIMIZE ... FINAL`
# always assigns `MergeType::Regular`, so these cases rely on background TTL merges.

function wait_for_ttl_drop_merge()
{
    local table=$1
    for _ in $(seq 1 300); do
        ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"
        local count
        count=$(${CLICKHOUSE_CLIENT} -q "
            SELECT count() FROM system.part_log
            WHERE database = currentDatabase() AND table = '$table'
              AND event_type = 'MergeParts' AND merge_reason = 'TTLDropMerge'")
        if [ "$count" -gt "0" ]; then
            return
        fi
        sleep 0.1
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
    SELECT max(length(merged_from)) < 3 FROM system.part_log
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

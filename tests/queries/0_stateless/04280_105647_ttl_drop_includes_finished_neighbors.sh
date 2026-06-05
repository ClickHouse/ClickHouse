#!/usr/bin/env bash

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/106021
#
# An earlier follow-up on this PR narrowed `TTLPartDropMergeSelector`'s
# `canConsiderPart` gate to exclude finished parts from being merge
# CENTERS, so the scheduler does not re-pick them on every tick. But that
# same gate was being consulted to decide whether to EXTEND a chosen range
# into a NEIGHBOR — so a fresh insert that produces an unfinished part
# could not be merged with an adjacent finished part via the TTL selector,
# only via `RegularMerge`.
#
# The fix splits the gate into `canConsiderPart` (still strict, for center
# eligibility) and `canIncludeInRange` (loose, allowing finished
# neighbors with a valid `general_ttl_info`). With the fix, a TTL drop /
# delete merge can sweep both an unfinished center and an adjacent
# finished neighbor in one go.
#
# The trigger MUST be `OPTIMIZE TABLE t` WITHOUT `FINAL` — `FINAL` goes
# through `selectAllPartsToMergeWithinPartition` and produces a
# `RegularMerge`, bypassing `ITTLMergeSelector` entirely. Non-FINAL
# without a `PARTITION` clause routes through `tryChooseTTLMerge` which
# is the only path that exercises `canIncludeInRange`. With
# `merge_with_ttl_timeout = 0` the background scheduler also drives the
# same path opportunistically.
#
# This is a shell test (not `.sql`) because:
#  (1) `OPTIMIZE TABLE t` is asynchronous — it enqueues the merge but
#      does not wait for completion. We poll `system.parts` until both
#      source parts have been consumed.
#  (2) The fold can be triggered by either the foreground OPTIMIZE or by
#      the background scheduler (both paths reach `canIncludeInRange`).
#      Whichever fires first is fine. The discriminating signal is the
#      EXISTENCE of a TTL-driven merge whose `merged_from` includes both
#      an `all_1_*` (finished part A) and an `all_2_*` (unfinished part
#      B) source — neither path produces that without the fix.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_ttl_drop_includes_finished_neighbors"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_ttl_drop_includes_finished_neighbors
(
    \`key\` UInt32,
    \`ts\` DateTime,
    \`value\` UInt32
)
ENGINE = MergeTree
ORDER BY (key)
TTL ts + INTERVAL 1 SECOND GROUP BY key SET value = sum(value)
SETTINGS min_bytes_for_wide_part = 0,
         merge_with_ttl_timeout = 0,
         max_number_of_merges_with_ttl_in_pool = 100,
         min_parts_to_merge_at_once = 100;
"

# Part A: 500 expired keys. `OPTIMIZE FINAL` on a single part drives the
# TTL transform and marks the resulting part as finished. After this the
# active part is named `all_1_1_*` (level >= 1).
$CLICKHOUSE_CLIENT --query "
INSERT INTO t_ttl_drop_includes_finished_neighbors
SELECT number AS key, toDateTime('2000-01-01 00:00:00') AS ts, 1 AS value
FROM numbers(1, 500);
"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_ttl_drop_includes_finished_neighbors FINAL"

# Part B: 500 expired keys, fresh. New part is `all_2_2_0` with
# `group_by_ttl[k].finished = 0`.
$CLICKHOUSE_CLIENT --query "
INSERT INTO t_ttl_drop_includes_finished_neighbors
SELECT number AS key, toDateTime('2000-01-02 00:00:00') AS ts, 1 AS value
FROM numbers(501, 500);
"

# Drive the TTL selector via non-FINAL OPTIMIZE. The background scheduler
# may race ahead — that's fine, both paths use the same selector and
# either firing the fold is acceptable. Wait until the merge completes by
# polling for active part count == 1 (both source parts consumed). The
# polling avoids the fixed-sleep race the previous version had.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_ttl_drop_includes_finished_neighbors" 2>/dev/null || true

for _ in $(seq 1 60); do
    ACTIVE=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.parts
        WHERE database = currentDatabase()
          AND table = 't_ttl_drop_includes_finished_neighbors'
          AND active")
    if [ "$ACTIVE" = "1" ]; then
        break
    fi
    sleep 0.25
done

# Let part_log catch the merge-completion event.
sleep 2
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS part_log"

# Discriminating assertion: there exists a completed TTL-driven merge
# whose `merged_from` array contains both an `all_1_*` (part A) and an
# `all_2_*` (part B) source name. Without the fix the selector cannot
# extend the range across A and B, so the only possible 2-part merge
# spanning both A and B is `RegularMerge` — which is excluded by the
# `merge_reason IN (...)` filter.
$CLICKHOUSE_CLIENT --query "
SELECT countIf(
    event_type = 'MergeParts'
    AND merge_reason IN ('TTLDropMerge', 'TTLDeleteMerge')
    AND arrayExists(x -> startsWith(x, 'all_1_'), merged_from)
    AND arrayExists(x -> startsWith(x, 'all_2_'), merged_from)
) > 0 AS ttl_merge_folded_finished_with_unfinished
FROM system.part_log
WHERE database = currentDatabase()
  AND table = 't_ttl_drop_includes_finished_neighbors';
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_ttl_drop_includes_finished_neighbors"

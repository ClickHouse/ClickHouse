#!/usr/bin/env bash

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/105647
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
# without a `PARTITION` clause routes through `tryChooseTTLMerge` which is
# the only path that exercises `canIncludeInRange`.
#
# This is a shell test rather than `.sql` for two reasons:
#  (1) `OPTIMIZE TABLE t` is asynchronous — it enqueues the merge but
#      does not wait for completion. We poll `system.parts` until the
#      active count drops to 1 (both source parts consumed).
#  (2) `merge_with_ttl_timeout = 0` makes the background scheduler pick
#      up new parts opportunistically. Without `SYSTEM STOP MERGES`
#      between the INSERT of part B and the snapshot of `part_log`, the
#      background scheduler can race the snapshot and the test sees a
#      pre-fold count instead of a zero baseline.

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
SETTINGS min_bytes_for_wide_part = 0, merge_with_ttl_timeout = 0;
"

# Part A: 500 expired keys, force it through TTL aggregation so it ends
# up marked finished. `OPTIMIZE FINAL` on a single part is the only way
# to deterministically drive the TTL transform without first growing the
# part count.
$CLICKHOUSE_CLIENT --query "
INSERT INTO t_ttl_drop_includes_finished_neighbors
SELECT number AS key, toDateTime('2000-01-01 00:00:00') AS ts, 1 AS value
FROM numbers(1, 500);
"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_ttl_drop_includes_finished_neighbors FINAL"

# Stop background merges before adding Part B. With merge_with_ttl_timeout
# = 0 the background scheduler will race and fire the fold between the
# INSERT and the snapshot below, leaving the snapshot count already
# bumped and the post-OPTIMIZE delta = 0.
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES t_ttl_drop_includes_finished_neighbors"

# Part B: 500 expired keys, fresh. New part -> group_by_ttl[k] entry with
# finished = 0. With Part A still active and finished, the post-fix TTL
# selector will pick B as a center and extend the range to include A via
# canIncludeInRange.
$CLICKHOUSE_CLIENT --query "
INSERT INTO t_ttl_drop_includes_finished_neighbors
SELECT number AS key, toDateTime('2000-01-02 00:00:00') AS ts, 1 AS value
FROM numbers(501, 500);
"

# Snapshot the count of multi-part TTL merges so far. system.part_log is
# shared across stateless-test runs and across both OPTIMIZE FINAL above
# (which produces 1-part TTL merges) and any later events; without a
# snapshot prior counts leak into the assertion.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS part_log"
SNAPSHOT=$($CLICKHOUSE_CLIENT --query "
SELECT countIf(
    event_type = 'MergeParts'
    AND merge_reason IN ('TTLDropMerge', 'TTLDeleteMerge')
    AND length(merged_from) >= 2
)
FROM system.part_log
WHERE database = currentDatabase()
  AND table = 't_ttl_drop_includes_finished_neighbors';
")

# Resume merges and drive the TTL selector synchronously-from-SQL.
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES t_ttl_drop_includes_finished_neighbors"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_ttl_drop_includes_finished_neighbors"

# OPTIMIZE returned. The merge runs in the background. Poll until both
# source parts have been consumed (active count drops to 1). Tolerate up
# to ~15s of latency.
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

# Give part_log a moment to flush asynchronous merge-completion events,
# then force a flush.
sleep 2
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS part_log"

# Discriminating assertion: at least one NEW completed TTLDropMerge or
# TTLDeleteMerge (beyond the snapshot) consumed >= 2 source parts.
# Before the fix this delta is 0 (pre-fix selector only produces 1-part
# TTL merges in this scenario). With the fix it is >= 1.
$CLICKHOUSE_CLIENT --query "
SELECT
    (
        SELECT countIf(
            event_type = 'MergeParts'
            AND merge_reason IN ('TTLDropMerge', 'TTLDeleteMerge')
            AND length(merged_from) >= 2
        )
        FROM system.part_log
        WHERE database = currentDatabase()
          AND table = 't_ttl_drop_includes_finished_neighbors'
    ) > $SNAPSHOT AS ttl_merge_folded_finished_with_unfinished;
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_ttl_drop_includes_finished_neighbors"

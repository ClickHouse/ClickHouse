#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-random-settings
#
# min_ttl_age_to_delete_merge_seconds must gate every part in a TTLDelete merge, not only the
# part the range is centred on. Before the fix, findCenters applied the age gate but
# findLeftRangeBorder/findRightRangeBorder did not, so a merge centred on a long-expired part
# still pulled in an adjacent part whose rows had only just expired.
#
# The assertion is on merge COMPOSITION, not on which rows survive: every merge purges expired
# rows from the parts it touches, so a plain background merge would delete the young part's
# rows too and tell us nothing about the selector. part_log.merged_from is what actually
# answers "did the young part participate in the TTLDelete merge".

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_min_ttl_age"

# merge_with_ttl_timeout = 0 takes the in-memory per-partition cooldown out of the picture, so
# the age gate is the only thing deciding eligibility.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_min_ttl_age (d DateTime, tag String)
ENGINE = MergeTree ORDER BY tag
TTL d + INTERVAL 1 SECOND DELETE
SETTINGS min_ttl_age_to_delete_merge_seconds = 3600,
         merge_with_ttl_timeout = 0,
         min_bytes_for_wide_part = 0;"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_min_ttl_age"

# 'old'   — expired two days ago, far past the 3600s gate: a valid merge centre.
# 'young' — expired seconds ago, well inside the gate: must not join old's TTLDelete merge.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_min_ttl_age VALUES (now() - INTERVAL 2 DAY, 'old')"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_min_ttl_age VALUES (now() - INTERVAL 5 SECOND, 'young')"

$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_min_ttl_age"

for _ in {0..150}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS part_log" 2>/dev/null
    n=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.part_log
        WHERE database = currentDatabase() AND table = 't_min_ttl_age'
          AND event_type = 'MergeParts' AND merge_reason = 'TTLDeleteMerge'")
    if [ "$n" != "0" ]; then
        break
    fi
    sleep 0.3
done

# Every TTLDelete merge must have taken exactly one source part — the old one. Without the
# gate on range expansion this reports 2 source parts. Reported distinctly from "no TTLDelete
# merge happened at all", which would mean the test never exercised the path.
$CLICKHOUSE_CLIENT -q "
SELECT multiIf(
        count() = 0, 'no TTLDelete merge observed',
        countIf(length(merged_from) != 1) > 0, 'young part joined the merge',
        'only the old part merged')
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_min_ttl_age'
  AND event_type = 'MergeParts' AND merge_reason = 'TTLDeleteMerge'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_min_ttl_age"

#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-random-settings
#
# min_ttl_age_to_delete_merge_seconds must gate every part in a TTLDelete merge, not just the
# part the range is centred on. A merge centred on a long-expired part must NOT drag in an
# adjacent part whose rows expired a moment ago and delete those rows early.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_min_ttl_age"

# merge_with_ttl_timeout = 0 removes the in-memory per-partition cooldown from the picture, so
# the only thing deciding eligibility is the new age gate.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_min_ttl_age (d DateTime, tag String)
ENGINE = MergeTree ORDER BY tag
TTL d + INTERVAL 1 SECOND DELETE
SETTINGS min_ttl_age_to_delete_merge_seconds = 3600,
         merge_with_ttl_timeout = 0,
         min_bytes_for_wide_part = 0;"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_min_ttl_age"

# 'old'   — expired two days ago, far past the 3600s gate: a valid merge centre.
# 'young' — expired seconds ago, well inside the gate: must survive this round.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_min_ttl_age VALUES (now() - INTERVAL 2 DAY, 'old')"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_min_ttl_age VALUES (now() - INTERVAL 5 SECOND, 'young')"

echo "before: $($CLICKHOUSE_CLIENT -q "SELECT groupArray(tag) FROM (SELECT tag FROM t_min_ttl_age ORDER BY tag)")"

$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_min_ttl_age"

# Wait for the TTLDelete merge to reclaim 'old'. Bounded: if it never happens the final
# SELECT still reports what is there and the reference catches it.
for _ in {0..100}; do
    remaining=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM t_min_ttl_age WHERE tag = 'old'")
    if [ "$remaining" = "0" ]; then
        break
    fi
    sleep 0.3
done

# 'old' reclaimed, 'young' untouched — the gate held on the neighbour.
$CLICKHOUSE_CLIENT -q "SELECT tag FROM t_min_ttl_age ORDER BY tag"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_min_ttl_age"

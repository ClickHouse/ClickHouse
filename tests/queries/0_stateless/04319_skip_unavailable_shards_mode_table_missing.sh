#!/usr/bin/env bash
# Tags: no-parallel, no-flaky-check, shard

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# --- TEMPORARY DEBUG INSTRUMENTATION -----------------------------------------------------------
# This test intermittently hangs on Darwin CI (~33% of runs) with `DROP DATABASE shard_0;`
# blocking for 60+ seconds. This .sh version is a temporary debug build: it runs a background
# poller (a separate connection) dumping system.processes / system.stack_trace every few seconds,
# spanning the failing `unavailable`-mode INSERT through the DROP, so we capture the orphaned
# shard_0 sub-insert thread's state *during* an actual hang on a real CI run, not just before it.
# Revert to the plain .sql file once we have a CI sample.
# ------------------------------------------------------------------------------------------------

$CLICKHOUSE_CLIENT --multiquery --query "
DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;
CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

CREATE TABLE shard_0.t_sus (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO shard_0.t_sus VALUES (10), (20);

CREATE TABLE dist_sus (x UInt32) ENGINE = Distributed('test_cluster_two_shards_different_databases', '', t_sus, x);

INSERT INTO dist_sus SELECT number FROM numbers(4)
SETTINGS distributed_foreground_insert = 1, skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_table_missing';

SELECT x FROM shard_0.t_sus ORDER BY x;

INSERT INTO dist_sus SELECT number + 100 FROM numbers(4)
SETTINGS distributed_foreground_insert = 1, skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing';

SELECT x FROM shard_0.t_sus ORDER BY x;
"

# Background poller: dumps system.processes / system.stack_trace every 3s, from right before the
# failing insert until this script kills it (after DROP DATABASE shard_0 returns, or ~90s max as
# a safety net so it can't outlive the test).
#
# Output is kept deliberately compact (TSVRaw, truncated stacks, and stack_trace filtered down to
# only queries that have already run suspiciously long) so that ~30 snapshots spanning the full
# poll window fit inside the CI harness's ~16KB captured-output cap on a timeout kill. Previously
# each snapshot used FORMAT Vertical with full demangled stacks for every thread on the server
# (including unrelated concurrently-running fast-test queries), which alone exceeded the cap after
# 1-2 iterations -- so only the very first (t=+0s, uninformative) snapshot ever made it into a CI
# report, never the state right before the actual hang/kill.
(
    START=$(date +%s)
    while :; do
        NOW=$(date +%s)
        echo "t+${NOW}-${START}=$((NOW - START))s" >&2
        $CLICKHOUSE_CLIENT --query "
        SELECT query_id, elapsed, substring(query, 1, 60)
        FROM system.processes
        WHERE query NOT LIKE '%system.processes%'
        ORDER BY elapsed DESC
        FORMAT TSVRaw
        " >&2 2>&1 ||:
        $CLICKHOUSE_CLIENT --query "
        SET allow_introspection_functions = 1;
        SELECT
            thread_id,
            query_id,
            arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), arraySlice(trace, 1, 5)), ' <- ') AS top_frames
        FROM system.stack_trace
        WHERE query_id IN (SELECT query_id FROM system.processes WHERE elapsed > 2)
        FORMAT TSVRaw
        " >&2 2>&1 ||:
        if [ $((NOW - START)) -ge 90 ]; then
            break
        fi
        sleep 3
    done
) &
POLLER_PID=$!

# `unavailable`: the missing table is an error. This is the query after which the orphaned
# shard_0 sub-insert has historically been left dangling.
$CLICKHOUSE_CLIENT --query "
INSERT INTO dist_sus SELECT number FROM numbers(4)
SETTINGS distributed_foreground_insert = 1, skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable'
" 2>&1 | grep -o "UNKNOWN_TABLE" ||:

echo "--- DEBUG: issuing DROP TABLE / DROP DATABASE, timing it ---" >&2
DROP_START=$(date +%s)
$CLICKHOUSE_CLIENT --query "DROP TABLE dist_sus;"
$CLICKHOUSE_CLIENT --query "DROP DATABASE shard_0;"
DROP_END=$(date +%s)
echo "--- DEBUG: DROP DATABASE shard_0 took $((DROP_END - DROP_START))s ---" >&2

kill "$POLLER_PID" 2>/dev/null
wait "$POLLER_PID" 2>/dev/null

$CLICKHOUSE_CLIENT --query "DROP DATABASE shard_1;"

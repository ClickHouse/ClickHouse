#!/usr/bin/env bash
# Tags: no-ordinary-database, no-random-settings, no-random-merge-tree-settings

# no-random-settings / no-random-merge-tree-settings: this is a plan-time type-drift race on
# empty ENGINE=Memory tables, unrelated to any query or MergeTree setting. With randomization on,
# the CI-injected settings inflate per-client memory so the many concurrent EXCHANGE/SELECT clients
# get OOM-killed on the memory-constrained sanitizer runners, which is not what the test targets.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The abort this test targets happens in ActionsDAG::updateHeader -> evaluatePartialResult
# (input_rows_count 0/1) during query planning, so it does not need any rows: a concurrent
# EXCHANGE TABLES swapping the column type between analysis and header computation is enough.
# Keep the tables empty - the flaky check reruns this test many times under sanitizers, and
# large Memory-engine inserts multiplied by that repetition exhaust the job's memory/time.

# Peak concurrency is bounded to keep the memory-constrained sanitizer runners from OOM-killing the
# clients (signal 9). Instead of launching every round at once (tens of live clickhouse-client
# processes per batch), each batch runs in small waves: a wave starts a couple of racing queries
# plus one EXCHANGE concurrently, then is fully awaited before the next wave. Only a handful of
# client processes are alive at a time, while the analysis-vs-EXCHANGE race window still exists
# within each wave; more waves recover the sampling that fewer-per-wave gives up.
WAVES=16

# Run one racing probe query and classify its outcome explicitly. The abort this PR eliminates is
# a type-mismatch LOGICAL_ERROR ("Unexpected return type ..."). It has exactly two manifestations:
# in a release build it is a catchable exception whose text reaches the client, so we fail on that
# text; in a debug/sanitizer build it aborts the whole server process, which the liveness check
# after each batch detects directly (see assert_alive). We do NOT try to classify the abort from
# other client-side symptoms: a recoverable ILLEGAL_TYPE_OF_ARGUMENT (Code 43) is the expected
# non-abort outcome of the race (after the EXCHANGE settles, real execution re-resolves the
# function and reports a clean recoverable error), and a transient connection reset under heavy
# concurrency leaves the server up - both must be tolerated, and the liveness check tells crashes
# apart from them without false positives.
probe() {
    local out
    out=$(${CLICKHOUSE_CLIENT} --query "$1" 2>&1) || true
    if echo "$out" | grep -qE "LOGICAL_ERROR|Unexpected return type"; then
        echo "FAIL: fatal type-mismatch abort during partial evaluation:" >&2
        echo "$out" >&2
        return 1
    fi
    return 0
}

# Run one EXCHANGE TABLES swap. Its exit code is what the batch inspects: the regression only
# exists when the schema swap actually happens between analysis and partial evaluation, so a
# killed/reset exchange must not silently leave the batch green.
exchange() {
    ${CLICKHOUSE_CLIENT} --query "EXCHANGE TABLES $1 AND $2" >/dev/null 2>&1
}

# Wait for every captured probe PID and fail if any probe reported a failure. A bare "wait"
# returns 0 even when a background job exited non-zero, so the PIDs are checked one by one.
check_pids() {
    local status=0 p
    for p in "$@"; do
        wait "$p" || status=1
    done
    return $status
}

# Unambiguous crash detector: if the type-mismatch abort fired in a debug/sanitizer build it killed
# the server, so a plain liveness query fails. A tolerated recoverable exception or a transient
# reset during the race leaves the server responding, so this has no false positives.
assert_alive() {
    ${CLICKHOUSE_CLIENT} --query "SELECT 1" >/dev/null 2>&1 \
        || { echo "FAIL: server not responding after race (partial-evaluation abort?)" >&2; exit 1; }
}

# Require that at least one EXCHANGE swap completed across the batch. If every exchange failed the
# schema never swapped (or the server went down) and the batch must not pass. A crash is still
# caught by assert_alive; this only guarantees the drift was exercised.
assert_drift_exercised() {
    [ "$1" = 1 ] || { echo "FAIL: no EXCHANGE TABLES swap completed in this batch (drift not exercised)" >&2; exit 1; }
}

# Base-type drift: swap Float64 with Int256. A strict function (arithmetic) resolved for the
# pre-EXCHANGE type would trip a LOGICAL_ERROR when handed the drifted type during partial
# evaluation, aborting the server in debug/sanitizer builds.
${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_1;
DROP TABLE IF EXISTS tbl_03007_2;
CREATE TABLE tbl_03007_1 (n Float64) ENGINE=Memory;
CREATE TABLE tbl_03007_2 (n Int256) ENGINE=Memory;
EOF

exchanged=0
for _ in $(seq 1 $WAVES); do
    pids=(); xpids=()
    probe "SELECT n * 0.123 FROM (SELECT * FROM tbl_03007_1)" & pids+=($!)
    exchange tbl_03007_1 tbl_03007_2 & xpids+=($!)
    check_pids "${pids[@]}"
    for p in "${xpids[@]}"; do wait "$p" && exchanged=1; done
done
assert_drift_exercised "$exchanged"
assert_alive

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_1;
DROP TABLE IF EXISTS tbl_03007_2;
EOF

# Wrapper-only drift: swap String with Nullable(String). Partial evaluation compares each
# argument type against the type the function was resolved for by exact equality, so a
# wrapper-only drift is do-not-fold too. This covers two otherwise-distinct hazards:
#   - materialize returns its argument type, so under drift it would produce a column whose
#     type differs from the resolved result type (Unexpected-return-type LOGICAL_ERROR);
#   - isNullable is a wrapper-sensitive value folder (folds UInt8(0/1) straight from the
#     argument type), so under drift its result type stays UInt8 and no result-type check
#     would catch a wrong fold: without the exact-type guard it would hand the one-row
#     partial-evaluation callers a definitive but wrong value instead of "unknown".
${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_3;
DROP TABLE IF EXISTS tbl_03007_4;
CREATE TABLE tbl_03007_3 (s String) ENGINE=Memory;
CREATE TABLE tbl_03007_4 (s Nullable(String)) ENGINE=Memory;
EOF

exchanged=0
for _ in $(seq 1 $WAVES); do
    pids=(); xpids=()
    probe "SELECT materialize(s) FROM (SELECT * FROM tbl_03007_3)" & pids+=($!)
    probe "SELECT isNullable(s) FROM (SELECT * FROM tbl_03007_3)" & pids+=($!)
    exchange tbl_03007_3 tbl_03007_4 & xpids+=($!)
    check_pids "${pids[@]}"
    for p in "${xpids[@]}"; do wait "$p" && exchanged=1; done
done
assert_drift_exercised "$exchanged"
assert_alive

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_3;
DROP TABLE IF EXISTS tbl_03007_4;
EOF

# Wrapper-only drift reaching an input_rows_count == 1 partial-evaluation caller. The queries
# above go through ActionsDAG::updateHeader (input_rows_count == 0); the one-row path that the
# do-not-fold guard also protects is reached only from the partial-evaluation callers (JOIN
# rewrite / shard skipping / path extraction). Here an ANY LEFT JOIN whose post-join filter is
# isNullable(r.w) triggers convertAnyJoinToSemiOrAntiJoin, which evaluates that filter via
# evaluatePartialResult(input_rows_count = 1) to decide the rewrite. A concurrent EXCHANGE
# swapping String with Nullable(String) drifts r.w between analysis and that evaluation: without
# the exact-type guard isNullable would fold a definitive but wrong UInt8, silently changing the
# rewrite; with the guard the drifted argument is do-not-fold, the column stays null and the
# caller takes its "unknown" path (getFilterResult returns UNKNOWN) so the JOIN is left unchanged.
${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_5;
DROP TABLE IF EXISTS tbl_03007_6;
DROP TABLE IF EXISTS tbl_03007_l;
CREATE TABLE tbl_03007_l (k UInt64) ENGINE=Memory;
CREATE TABLE tbl_03007_5 (k UInt64, w String) ENGINE=Memory;
CREATE TABLE tbl_03007_6 (k UInt64, w Nullable(String)) ENGINE=Memory;
EOF

exchanged=0
for _ in $(seq 1 $WAVES); do
    pids=(); xpids=()
    probe "SELECT l.k FROM tbl_03007_l l ANY LEFT JOIN tbl_03007_5 r ON l.k = r.k WHERE isNullable(r.w) SETTINGS query_plan_convert_any_join_to_semi_or_anti_join = 1" & pids+=($!)
    exchange tbl_03007_5 tbl_03007_6 & xpids+=($!)
    check_pids "${pids[@]}"
    for p in "${xpids[@]}"; do wait "$p" && exchanged=1; done
done
assert_drift_exercised "$exchanged"
assert_alive

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_5;
DROP TABLE IF EXISTS tbl_03007_6;
DROP TABLE IF EXISTS tbl_03007_l;
EOF

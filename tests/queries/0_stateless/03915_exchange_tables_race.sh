#!/usr/bin/env bash
# Tags: no-ordinary-database

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The abort this test targets happens in ActionsDAG::updateHeader -> evaluatePartialResult
# (input_rows_count 0/1) during query planning, so it does not need any rows: a concurrent
# EXCHANGE TABLES swapping the column type between analysis and header computation is enough.
# Keep the tables empty - the flaky check reruns this test many times under sanitizers, and
# large Memory-engine inserts multiplied by that repetition exhaust the job's memory/time.

# Run one racing probe query and classify its outcome explicitly. Returns non-zero (fails the
# test) only for the failure this PR eliminates: a fatal type-mismatch abort, or a lost server
# connection (how such an abort surfaces to the client). A recoverable server exception such as
# ILLEGAL_TYPE_OF_ARGUMENT (Code 43) is an expected, acceptable outcome of the race - after the
# EXCHANGE settles the real execution re-resolves the function and reports a clean recoverable
# error - so we must not demand unconditional client success, only the absence of an abort.
probe() {
    local out
    out=$(${CLICKHOUSE_CLIENT} --query "$1" 2>&1) || true
    if echo "$out" | grep -qE "LOGICAL_ERROR|Unexpected return type"; then
        echo "FAIL: fatal type-mismatch abort during partial evaluation:" >&2
        echo "$out" >&2
        return 1
    fi
    if echo "$out" | grep -qiE "Connection refused|Connection reset|Broken pipe|I/O error|DB::NetException|Poco::(Net|TimeoutException)|Timeout: connect"; then
        echo "FAIL: lost connection to server (possible crash):" >&2
        echo "$out" >&2
        return 1
    fi
    return 0
}

# Wait for every captured background PID and fail if any probe reported a failure. A bare "wait"
# returns 0 even when a background job exited non-zero, so the PIDs are checked one by one.
check_pids() {
    local status=0 p
    for p in "$@"; do
        wait "$p" || status=1
    done
    return $status
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

pids=()
for _ in {1..10}; do
    probe "SELECT n * 0.123 FROM (SELECT * FROM tbl_03007_1)" & pids+=($!)
    ${CLICKHOUSE_CLIENT} --query "EXCHANGE TABLES tbl_03007_1 AND tbl_03007_2" 2>/dev/null &
done
check_pids "${pids[@]}"

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

pids=()
for _ in {1..10}; do
    probe "SELECT materialize(s) FROM (SELECT * FROM tbl_03007_3)" & pids+=($!)
    probe "SELECT isNullable(s) FROM (SELECT * FROM tbl_03007_3)" & pids+=($!)
    ${CLICKHOUSE_CLIENT} --query "EXCHANGE TABLES tbl_03007_3 AND tbl_03007_4" 2>/dev/null &
done
check_pids "${pids[@]}"

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

pids=()
for _ in {1..10}; do
    probe "SELECT l.k FROM tbl_03007_l l ANY LEFT JOIN tbl_03007_5 r ON l.k = r.k WHERE isNullable(r.w) SETTINGS query_plan_convert_any_join_to_semi_or_anti_join = 1" & pids+=($!)
    ${CLICKHOUSE_CLIENT} --query "EXCHANGE TABLES tbl_03007_5 AND tbl_03007_6" 2>/dev/null &
done
check_pids "${pids[@]}"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS tbl_03007_5;
DROP TABLE IF EXISTS tbl_03007_6;
DROP TABLE IF EXISTS tbl_03007_l;
EOF

#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Cancellation during CTE resolution must unwind CTE cleanup guards cleanly with
# TIMEOUT_EXCEEDED without recomputing tree hashes or terminating the server process.
# Covers all 5 CTE cleanup sites: join-tree CTE, expression CTE, expression materialized CTE,
# and join-tree materialized CTE (both first and subsequent references).
#
# SQL payloads and client error streams are kept in files under CLICKHOUSE_TMP so `bash -x`
# under CI flaky-check never traces large query strings.

TMP_SQL_DIR="${CLICKHOUSE_TMP}/cte_cancel_${CLICKHOUSE_DATABASE}_$$"
mkdir -p "$TMP_SQL_DIR"
trap 'rm -rf "$TMP_SQL_DIR"' EXIT

# Generate a single-column CTE with ~66,050 (> 65,536 = 1 << 16) QueryTree nodes split into
# 3 tuple groups of 7,340 `number + k` expressions, each wrapped in 4 nested `randConstant` calls:
# - Parsing/building only 22,020 expressions keeps 8-way parallel CI flaky-check well within 180s.
# - Each `randConstant` invokes `getTreeHash()` during `resolveQuery` (`resolveFunction.cpp`), visiting
#   4 * 66,050 = >264,000 nodes (~4 cancellation checkpoints evenly spaced inside `CTEInResolveProcessGuard`).
# - The CTE itself exceeds 65,536 nodes so any raw-node re-hash during scope-exit unwinding crosses
#   `cancellation_check_period` in `IQueryTreeNode::getTreeHash()` and aborts an unfixed server.
awk -v dir="$TMP_SQL_DIR" 'BEGIN {
    idx = 0;
    sum_expr = "";
    for (g = 1; g <= 3; g++) {
        if (g > 1) sum_expr = sum_expr "+";
        inner = "tuple(";
        for (k = 1; k <= 7340; k++) {
            idx++;
            if (k > 1) inner = inner ",";
            inner = inner "number+" idx;
        }
        inner = inner ")";
        expr = inner;
        for (d = 1; d <= 4; d++) {
            expr = "randConstant(tuple(" (g * 10 + d) "," expr "))";
        }
        sum_expr = sum_expr expr;
    }
    body = "SELECT toUInt64(" sum_expr ") AS v FROM numbers(0)";

    print "WITH c AS (" body ") SELECT 1 FORMAT Null;" > (dir "/calib_parse.sql");
    print "WITH c AS MATERIALIZED (" body ") SELECT v FROM c FORMAT Null;" > (dir "/calib_1ref.sql");
    print "WITH c AS (" body ") SELECT v FROM c FORMAT Null;" > (dir "/join.sql");
    print "WITH c AS (" body ") SELECT 1 WHERE toUInt64(0) IN c FORMAT Null;" > (dir "/expr.sql");
    print "WITH c AS MATERIALIZED (" body ") SELECT 1 WHERE toUInt64(0) IN c FORMAT Null;" > (dir "/expr_mat.sql");
    print "WITH c AS MATERIALIZED (" body ") SELECT v FROM c FORMAT Null;" > (dir "/join_mat_1.sql");

    u = "";
    for (b = 1; b <= 4; b++) {
        if (b > 1) u = u " UNION ALL ";
        u = u "SELECT v FROM c";
    }
    print "WITH c AS MATERIALIZED (" body ") " u " FORMAT Null;" > (dir "/join_mat_2.sql");
}'

COMMON_ARGS=(
    --enable_analyzer=1
    --enable_materialized_cte=1
    --max_query_size=10000000
    --max_ast_elements=2000000
    --max_expanded_ast_elements=2000000
    --log_queries=1
)

# Warm up connection and calibrate server-side parse time (T_PARSE_MS) vs post-parse single-CTE
# analysis+plan time (R1_MS = T_1REF_MS - T_PARSE_MS) directly from system.query_log.
# Note: `max_execution_time` starts after `parseQuery`, whereas `query_duration_ms` includes `parseQuery`.
QID_PARSE="${CLICKHOUSE_DATABASE}_cte_calib_parse_$$"
QID_1REF="${CLICKHOUSE_DATABASE}_cte_calib_1ref_$$"

$CLICKHOUSE_CLIENT --query_id "$QID_PARSE" "${COMMON_ARGS[@]}" --queries-file "$TMP_SQL_DIR/calib_parse.sql"
$CLICKHOUSE_CLIENT --query_id "$QID_1REF" "${COMMON_ARGS[@]}" --queries-file "$TMP_SQL_DIR/calib_1ref.sql"

read -r T_PARSE_MS T_1REF_MS < <($CLICKHOUSE_CLIENT -n -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT
        maxIf(query_duration_ms, query_id = '$QID_PARSE'),
        maxIf(query_duration_ms, query_id = '$QID_1REF')
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND query_id IN ('$QID_PARSE', '$QID_1REF')
      AND type = 'QueryFinish'")

T_PARSE_MS=${T_PARSE_MS:-10}
T_1REF_MS=${T_1REF_MS:-100}
R1_MS=$((T_1REF_MS - T_PARSE_MS))
if [ "$R1_MS" -lt 40 ]; then
    R1_MS=40
fi

WINNING_QIDS=()

run_cancelled_cte()
{
    local label="$1"
    local sql_file="$2"
    local min_r1_frac="$3"
    shift 3
    local attempt=0

    local min_srv_ms
    min_srv_ms=$(awk -v tp="$T_PARSE_MS" -v r1="$R1_MS" -v f="$min_r1_frac" 'BEGIN { print int(tp + r1 * f) }')

    for mult in "$@"; do
        attempt=$((attempt + 1))
        local timeout_s
        timeout_s=$(awk -v r1="$R1_MS" -v m="$mult" 'BEGIN {
            t = (r1 * m) / 1000.0;
            if (t < 0.015) t = 0.015;
            printf "%.3f", t;
        }')
        local qid="${CLICKHOUSE_DATABASE}_cte_cancel_${label}_${attempt}_$$"
        local err_file="$TMP_SQL_DIR/err_${label}_${attempt}.txt"

        local rc=0
        $CLICKHOUSE_CLIENT \
            --query_id "$qid" \
            "${COMMON_ARGS[@]}" \
            --max_execution_time="$timeout_s" \
            --timeout_overflow_mode='throw' \
            --queries-file "$sql_file" > "$err_file" 2>&1 || rc=$?

        if [ "$rc" -eq 0 ]; then
            continue
        fi

        if grep -qE 'Connection reset|Logical error|Aborted|NetException|EOF' "$err_file"; then
            printf 'Unexpected crash/disconnect for %s (timeout=%ss): ' "$label" "$timeout_s" >&2
            head -c 400 "$err_file" >&2
            printf '\n' >&2
            return 1
        fi

        if ! grep -q 'TIMEOUT_EXCEEDED' "$err_file"; then
            printf 'Unexpected non-159 error for %s (rc=%s, timeout=%ss): ' "$label" "$rc" "$timeout_s" >&2
            head -c 400 "$err_file" >&2
            printf '\n' >&2
            return 1
        fi

        local ok_row
        ok_row=$($CLICKHOUSE_CLIENT -n -q "
            SYSTEM FLUSH LOGS query_log;
            SELECT count()
            FROM system.query_log
            WHERE current_database = currentDatabase()
              AND query_id = '$qid'
              AND type = 'ExceptionBeforeStart'
              AND exception_code = 159
              AND query_duration_ms >= $min_srv_ms")

        if [ "${ok_row:-0}" -ge 1 ]; then
            echo "TIMEOUT_EXCEEDED"
            WINNING_QIDS+=("'$qid'")
            return 0
        fi
    done

    printf 'Failed to trigger in-scope ExceptionBeforeStart TIMEOUT_EXCEEDED for %s (T_PARSE_MS=%s, R1_MS=%s)\n' \
        "$label" "$T_PARSE_MS" "$R1_MS" >&2
    return 1
}

# Single-reference cases: `max_execution_time` starts after `parseQuery`, and `CTEInResolveProcessGuard`
# spans roughly [0.14*R1_MS, 0.45*R1_MS] before later optimizer passes run. Start at 0.25*R1_MS so
# cancellation hits strictly inside `CTEInResolveProcessGuard`.
run_cancelled_cte "join"       "$TMP_SQL_DIR/join.sql"       "0.12" 0.25 0.34 0.18 0.44 0.56
run_cancelled_cte "expr"       "$TMP_SQL_DIR/expr.sql"       "0.12" 0.25 0.34 0.18 0.44 0.56
run_cancelled_cte "expr_mat"   "$TMP_SQL_DIR/expr_mat.sql"   "0.12" 0.25 0.34 0.18 0.44 0.56
run_cancelled_cte "join_mat_1" "$TMP_SQL_DIR/join_mat_1.sql" "0.12" 0.25 0.34 0.18 0.44 0.56

# Subsequent materialized-reference case (4 UNION ALL branches):
# Reference #1 finishes `resolveQuery` and initializes `materialized_cte_ptr->storage` by ~0.45*R1_MS,
# and References #2..#4 resolve in [0.45*R1_MS, 1.80*R1_MS]. Require query_duration_ms >= T_PARSE_MS + 0.65*R1_MS
# and start at 0.95*R1_MS so cancellation hits strictly inside subsequent-reference cleanup.
run_cancelled_cte "join_mat_2" "$TMP_SQL_DIR/join_mat_2.sql" "0.65" 0.95 1.30 0.72 1.65 2.10

if [ "${#WINNING_QIDS[@]}" -eq 5 ]; then
    QID_LIST=$(IFS=,; echo "${WINNING_QIDS[*]}")
    $CLICKHOUSE_CLIENT -q "
        SELECT count(), countIf(type = 'ExceptionBeforeStart' AND exception_code = 159)
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND query_id IN ($QID_LIST)
          AND type != 'QueryStart'"
fi

$CLICKHOUSE_CLIENT -q "SELECT 1"

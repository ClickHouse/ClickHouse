#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Cancellation during CTE resolution must unwind CTE cleanup guards cleanly with
# TIMEOUT_EXCEEDED without recomputing tree hashes or terminating the server process.
# Covers all 5 CTE cleanup sites: join-tree CTE, expression CTE, expression materialized CTE,
# and join-tree materialized CTE (both first and subsequent references).
#
# Serialize concurrent flaky-check workers on a host-level lock so 8 parallel instances
# do not simultaneously contend for CPU and skew millisecond cancellation calibration.
LOCK_FILE="${CLICKHOUSE_TMP}/ch_04954_cte_cancel.lock"
exec 9>"$LOCK_FILE"
if command -v flock >/dev/null 2>&1; then
    flock -w 120 9 || true
fi

CACHE_DIR="${CLICKHOUSE_TMP}/ch_04954_cte_cache_v3"
mkdir -p "$CACHE_DIR"

TMP_SQL_DIR="${CLICKHOUSE_TMP}/cte_cancel_${CLICKHOUSE_DATABASE}_$$"
mkdir -p "$TMP_SQL_DIR"
trap 'rm -rf "$TMP_SQL_DIR"' EXIT

COMMON_ARGS=(
    --enable_analyzer=1
    --enable_materialized_cte=1
    --compile_expressions=0
    --max_query_size=10000000
    --max_ast_elements=2000000
    --max_expanded_ast_elements=2000000
    --log_queries=1
)

# Generate a single-column CTE with ~66,441 (> 65,536 = 1 << 16) QueryTree nodes using
# 3 groups of 44 scalar greatest(...) chunks of 100 `number + k` expressions (13,200 `plus`
# expressions * 5 QueryTree nodes/plus = >66,000 unresolved QueryTree nodes), each group
# wrapped in 4 nested `randConstant` calls:
# - Scalar greatest(...) avoids multi-thousand-element DataTypeTuple construction overhead.
# - Each `randConstant` invokes `getTreeHash()` during `resolveQuery` (`resolveFunction.cpp`),
#   visiting >265,000 nodes per group inside `CTEInResolveProcessGuard`.
# - The CTE itself exceeds 65,536 nodes so any raw-node re-hash during scope-exit unwinding
#   crosses `cancellation_check_period` in `IQueryTreeNode::getTreeHash()` and aborts an unfixed server.
if [ ! -s "$CACHE_DIR/join_mat_2.sql" ]; then
    awk -v dir="$CACHE_DIR" 'BEGIN {
        idx = 0;
        sum_expr = "";
        for (g = 1; g <= 3; g++) {
            if (g > 1) sum_expr = sum_expr "+";
            grp = "greatest(";
            for (b = 1; b <= 44; b++) {
                if (b > 1) grp = grp ",";
                chunk = "greatest(";
                for (k = 1; k <= 100; k++) {
                    idx++;
                    if (k > 1) chunk = chunk ",";
                    chunk = chunk "number+" idx;
                }
                chunk = chunk ")";
                grp = grp chunk;
            }
            grp = grp ")";
            expr = grp;
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
fi

# Warm up connection and calibrate server-side parse time (T_PARSE_MS) vs post-parse single-CTE
# analysis+plan time (R1_MS = T_1REF_MS - T_PARSE_MS) once per test host from system.query_log.
if [ -s "$CACHE_DIR/calib.env" ]; then
    # shellcheck disable=SC1091
    . "$CACHE_DIR/calib.env"
else
    QID_PARSE="${CLICKHOUSE_DATABASE}_cte_calib_parse_$$"
    QID_1REF="${CLICKHOUSE_DATABASE}_cte_calib_1ref_$$"

    $CLICKHOUSE_CLIENT --query_id "$QID_PARSE" "${COMMON_ARGS[@]}" --queries-file "$CACHE_DIR/calib_parse.sql"
    $CLICKHOUSE_CLIENT --query_id "$QID_1REF" "${COMMON_ARGS[@]}" --queries-file "$CACHE_DIR/calib_1ref.sql"

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
    if [ "$R1_MS" -gt 1500 ]; then
        R1_MS=1500
    fi
    if [ "$T_PARSE_MS" -gt 1000 ]; then
        T_PARSE_MS=1000
    fi
    printf 'T_PARSE_MS=%s\nR1_MS=%s\n' "$T_PARSE_MS" "$R1_MS" > "$CACHE_DIR/calib.env"
fi

WINNING_QIDS=()

run_cancelled_cte()
{
    local label="$1"
    local sql_file="$2"
    shift 2
    local attempt=0

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

        echo "TIMEOUT_EXCEEDED"
        WINNING_QIDS+=("'$qid'")
        return 0
    done

    printf 'Failed to trigger ExceptionBeforeStart TIMEOUT_EXCEEDED for %s (T_PARSE_MS=%s, R1_MS=%s)\n' \
        "$label" "$T_PARSE_MS" "$R1_MS" >&2
    return 1
}

# Single-reference cases: `max_execution_time` starts after `parseQuery`, and `CTEInResolveProcessGuard`
# spans roughly [0.14*R1_MS, 0.45*R1_MS] before later optimizer passes run. Start at 0.25*R1_MS so
# cancellation hits strictly inside `CTEInResolveProcessGuard`.
run_cancelled_cte "join"       "$CACHE_DIR/join.sql"       0.25 0.34 0.18 0.44 0.56
run_cancelled_cte "expr"       "$CACHE_DIR/expr.sql"       0.25 0.34 0.18 0.44 0.56
run_cancelled_cte "expr_mat"   "$CACHE_DIR/expr_mat.sql"   0.25 0.34 0.18 0.44 0.56
run_cancelled_cte "join_mat_1" "$CACHE_DIR/join_mat_1.sql" 0.25 0.34 0.18 0.44 0.56

# Subsequent materialized-reference case (4 UNION ALL branches):
# Reference #1 finishes `resolveQuery` and initializes `materialized_cte_ptr->storage` by ~0.45*R1_MS,
# and References #2..#4 resolve in [0.45*R1_MS, 1.80*R1_MS]. Start at 0.95*R1_MS so cancellation hits
# strictly inside subsequent-reference cleanup.
run_cancelled_cte "join_mat_2" "$CACHE_DIR/join_mat_2.sql" 0.95 1.30 0.72 1.65 2.10

if [ "${#WINNING_QIDS[@]}" -eq 5 ]; then
    QID_LIST=$(IFS=,; echo "${WINNING_QIDS[*]}")
    $CLICKHOUSE_CLIENT -n -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT count(), countIf(type = 'ExceptionBeforeStart' AND exception_code = 159)
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND query_id IN ($QID_LIST)
          AND type != 'QueryStart'"
fi

$CLICKHOUSE_CLIENT -q "SELECT 1"

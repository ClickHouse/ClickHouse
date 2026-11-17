#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Cancellation during CTE resolution must unwind CTE cleanup guards cleanly with
# TIMEOUT_EXCEEDED without recomputing tree hashes or terminating the server process.
# Covers all 5 CTE cleanup sites: join-tree CTE, expression CTE, expression materialized CTE,
# and join-tree materialized CTE (both first and subsequent references).

SHARED_TMP="${TMPDIR:-/tmp}"
CACHE_DIR="${SHARED_TMP}/ch_04954_cte_cache_v6"
LOCK_FILE="${SHARED_TMP}/ch_04954_cte_cancel_v6.lock"
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

# Hold the host-wide lock ONLY during one-time SQL generation and timing calibration so
# flaky-check parallel workers never serialize across the 5 cancelled queries.
exec 9>"$LOCK_FILE"
if command -v flock >/dev/null 2>&1; then
    flock -w 60 9 || true
fi

# Generate a single-column CTE whose:
# 1. Unresolved QueryTree (original_cte_node for sites 1, 3, 4, 5) has >66,000 (> 65,536 = 1 << 16)
#    QueryTree nodes via an unreferenced WITH alias (unused_pad of 22,000 zero-arg f() FunctionNodes,
#    3 QueryTree nodes each), which QueryAnalyzer drops in O(1) without resolving.
# 2. Resolved projection (for site 2 and for triggering getTreeHash() inside CTEInResolveProcessGuard)
#    expands a 14-step WITH alias chain (a0..a14 -> >65,536 QueryTree nodes) wrapped in 10 nested
#    randConstant calls so >650,000 nodes are hashed inside getTreeHash() while CTEInResolveProcessGuard
#    is active on the stack.
if [ ! -s "$CACHE_DIR/join_mat_2.sql" ]; then
    awk -v dir="$CACHE_DIR" 'BEGIN {
        pad = "tuple(";
        for (c = 1; c <= 100; c++) {
            if (c > 1) pad = pad ",";
            chunk = "tuple(";
            for (k = 1; k <= 220; k++) {
                if (k > 1) chunk = chunk ",";
                chunk = chunk "f()";
            }
            chunk = chunk ")";
            pad = pad chunk;
        }
        pad = pad ") AS unused_pad";

        dag = "number AS a0";
        for (d = 1; d <= 14; d++) {
            dag = dag ", greatest(a" (d - 1) ",a" (d - 1) ") AS a" d;
        }

        rc = "a14";
        for (r = 1; r <= 10; r++) {
            rc = "randConstant(tuple(" r "," rc "))";
        }

        body = "WITH " pad ", " dag " SELECT toUInt64(" rc ") AS v FROM numbers(0)";

        print "WITH c AS (" body ") SELECT 1 FORMAT Null;" > (dir "/calib_parse.sql");
        print "WITH c AS MATERIALIZED (" body ") SELECT v FROM c FORMAT Null;" > (dir "/calib_1ref.sql");
        print "WITH c AS (" body ") SELECT v FROM c FORMAT Null;" > (dir "/join.sql");
        print "WITH c AS (" body ") SELECT 1 WHERE toUInt64(0) IN c FORMAT Null;" > (dir "/expr.sql");
        print "WITH c AS MATERIALIZED (" body ") SELECT 1 WHERE toUInt64(0) IN c FORMAT Null;" > (dir "/expr_mat.sql");
        print "WITH c AS MATERIALIZED (" body ") SELECT v FROM c FORMAT Null;" > (dir "/join_mat_1.sql");
        print "WITH c AS MATERIALIZED (" body ") SELECT v FROM c UNION ALL SELECT v FROM c FORMAT Null;" > (dir "/join_mat_2.sql");
    }'
fi

# Calibrate post-parse single-CTE analysis time (R1_MS = T_1REF_MS - T_PARSE_MS) once per test host.
if [ ! -s "$CACHE_DIR/calib.env" ]; then
    QID_PARSE="${CLICKHOUSE_DATABASE}_cte_calib_parse_$$"
    QID_1REF="${CLICKHOUSE_DATABASE}_cte_calib_1ref_$$"

    $CLICKHOUSE_CLIENT -q "SELECT 1 FORMAT Null" >/dev/null 2>&1 || true
    $CLICKHOUSE_CLIENT --query_id "$QID_PARSE" "${COMMON_ARGS[@]}" --queries-file "$CACHE_DIR/calib_parse.sql" >/dev/null 2>&1 || true
    $CLICKHOUSE_CLIENT --query_id "$QID_1REF" "${COMMON_ARGS[@]}" --queries-file "$CACHE_DIR/calib_1ref.sql" >/dev/null 2>&1 || true

    read -r T_PARSE_MS T_1REF_MS < <($CLICKHOUSE_CLIENT -n -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT
            maxIf(query_duration_ms, query_id = '$QID_PARSE'),
            maxIf(query_duration_ms, query_id = '$QID_1REF')
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND query_id IN ('$QID_PARSE', '$QID_1REF')
          AND type = 'QueryFinish'")

    T_PARSE_MS=${T_PARSE_MS:-20}
    T_1REF_MS=${T_1REF_MS:-120}
    R1_MS=$((T_1REF_MS - T_PARSE_MS))
    if [ "$R1_MS" -lt 40 ]; then
        R1_MS=40
    fi
    printf 'T_PARSE_MS=%s\nR1_MS=%s\n' "$T_PARSE_MS" "$R1_MS" > "$CACHE_DIR/calib.env.tmp.$$"
    mv -f "$CACHE_DIR/calib.env.tmp.$$" "$CACHE_DIR/calib.env"
fi

if command -v flock >/dev/null 2>&1; then
    flock -u 9 || true
fi
exec 9>&-

# shellcheck disable=SC1091
. "$CACHE_DIR/calib.env"

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

        local t0 t1 elapsed_ms rc=0
        t0=$(date +%s%3N)
        $CLICKHOUSE_CLIENT \
            --query_id "$qid" \
            "${COMMON_ARGS[@]}" \
            --max_execution_time="$timeout_s" \
            --timeout_overflow_mode='throw' \
            --queries-file "$sql_file" > "$err_file" 2>&1 || rc=$?
        t1=$(date +%s%3N)
        elapsed_ms=$((t1 - t0))

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

        if [ "$elapsed_ms" -ge "$min_srv_ms" ]; then
            echo "TIMEOUT_EXCEEDED"
            WINNING_QIDS+=("'$qid'")
            return 0
        fi
    done

    printf 'Failed to trigger in-scope ExceptionBeforeStart TIMEOUT_EXCEEDED for %s (T_PARSE_MS=%s, R1_MS=%s)\n' \
        "$label" "$T_PARSE_MS" "$R1_MS" >&2
    return 1
}

# Single-reference cases: QueryStatus stopwatch starts after parseQuery; entry hash is at ~0.15*R1_MS
# and the 10 nested randConstant calls span [0.35*R1_MS, 0.98*R1_MS] inside CTEInResolveProcessGuard.
run_cancelled_cte "join"       "$CACHE_DIR/join.sql"       "0.15" 0.45 0.30 0.65 0.20 0.85
run_cancelled_cte "expr"       "$CACHE_DIR/expr.sql"       "0.15" 0.45 0.30 0.65 0.20 0.85
run_cancelled_cte "expr_mat"   "$CACHE_DIR/expr_mat.sql"   "0.15" 0.45 0.30 0.65 0.20 0.85
run_cancelled_cte "join_mat_1" "$CACHE_DIR/join_mat_1.sql" "0.15" 0.45 0.30 0.65 0.20 0.85

# Subsequent materialized-reference case (2 UNION ALL branches):
# Reference #1 finishes by ~1.00*R1_MS, and Reference #2's 10 randConstant calls span [1.25*R1_MS, 1.90*R1_MS].
run_cancelled_cte "join_mat_2" "$CACHE_DIR/join_mat_2.sql" "0.90" 1.35 1.15 1.60 1.00 1.85

if [ "${#WINNING_QIDS[@]}" -eq 5 ]; then
    QID_LIST=$(IFS=,; echo "${WINNING_QIDS[*]}")
    $CLICKHOUSE_CLIENT -n -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT count(), countIf(type = 'ExceptionBeforeStart' AND exception_code = 159)
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND query_id IN ($QID_LIST)
          AND type != 'QueryStart';
        SELECT 1;"
fi

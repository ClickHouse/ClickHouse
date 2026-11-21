#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Cancellation during CTE resolution must unwind CTE cleanup guards cleanly with
# TIMEOUT_EXCEEDED without recomputing tree hashes or terminating the server process.
# Covers all 5 CTE cleanup sites: join-tree CTE, expression CTE, expression materialized CTE,
# and join-tree materialized CTE (both first and subsequent references), plus allocation-free
# expression-stack SCOPE_EXIT cleanup under memory fault injection.

SHARED_TMP="${TMPDIR:-/tmp}"
CACHE_DIR="${SHARED_TMP}/ch_04954_cte_cache_v11"
LOCK_FILE="${SHARED_TMP}/ch_04954_cte_cancel_v11.lock"
mkdir -p "$CACHE_DIR"

TMP_SQL_DIR="${CLICKHOUSE_TMP}/cte_cancel_${CLICKHOUSE_DATABASE}_$$"
mkdir -p "$TMP_SQL_DIR"
trap 'rm -rf "$TMP_SQL_DIR"' EXIT

COMMON_ARGS=(
    --enable_analyzer=1
    --enable_materialized_cte=1
    --compile_expressions=0
    --log_queries=1
)

# Hold the host-wide lock ONLY during one-time SQL generation, fault-injection check, and timing calibration.
exec 9>"$LOCK_FILE"
if command -v flock >/dev/null 2>&1; then
    flock -w 60 9 || true
fi

if [ ! -s "$CACHE_DIR/join_mat_2.sql" ]; then
    awk -v dir="$CACHE_DIR" 'BEGIN {
        dag = "number AS a0";
        for (d = 1; d <= 14; d++) {
            dag = dag ", greatest(a" (d - 1) ",a" (d - 1) ") AS a" d;
        }

        rc = "a14";
        for (r = 1; r <= 4; r++) {
            rc = "randConstant(tuple(" r "," rc "))";
        }

        m_body = "WITH " dag " SELECT toUInt64(" rc ") AS v FROM numbers(0)";
        c_body = "WITH m AS MATERIALIZED (" m_body ") SELECT 1 AS v FROM numbers(0) WHERE toUInt64(0) IN m";

        # Trailing HAVING sentinel_after_cte is resolved immediately after FROM and WHERE inside
        # QueryAnalyzer::resolveQuery, ensuring any query that finishes CTE resolution without
        # hitting TIMEOUT_EXCEEDED fails fast with UNKNOWN_IDENTIFIER instead of entering execution
        # (ExceptionWhileProcessing).
        print "WITH c AS (" c_body ") SELECT 1 FORMAT Null;" > (dir "/calib_parse.sql");
        print "WITH c AS MATERIALIZED (" c_body ") SELECT v FROM c HAVING sentinel_after_cte FORMAT Null;" > (dir "/calib_1ref.sql");
        print "WITH c AS (" c_body ") SELECT v FROM c HAVING sentinel_after_cte FORMAT Null;" > (dir "/join.sql");
        print "WITH c AS (" c_body ") SELECT 1 WHERE toUInt64(0) IN c HAVING sentinel_after_cte FORMAT Null;" > (dir "/expr.sql");
        print "WITH c AS MATERIALIZED (" m_body ") SELECT 1 WHERE toUInt64(0) IN c HAVING sentinel_after_cte FORMAT Null;" > (dir "/expr_mat.sql");
        print "WITH c AS MATERIALIZED (" c_body ") SELECT v FROM c HAVING sentinel_after_cte FORMAT Null;" > (dir "/join_mat_1.sql");
        print "WITH c AS MATERIALIZED (" m_body ") SELECT v FROM c UNION ALL SELECT v FROM c HAVING sentinel_after_cte FORMAT Null;" > (dir "/join_mat_2.sql");

        fi_file = dir "/fault_inject.sql";
        print "SET enable_analyzer = 1;" > fi_file;
        print "SET calculate_text_stack_trace = 0;" > fi_file;
        print "SET max_untracked_memory = 0;" > fi_file;
        print "SET memory_tracker_fault_probability = 0.0002;" > fi_file;
        cols = "";
        for (i = 1; i <= 20; i++) {
            if (i > 1) cols = cols ", ";
            cols = cols sprintf("parseDateTime64BestEffortOrNull(\x272020-01-%02d\x27) AS c%d", i, i);
        }
        for (iter = 1; iter <= 6; iter++) {
            print "SELECT " cols " FORMAT Null;" > fi_file;
            print "WITH x AS a1, a1 AS a2, a2 AS a3 SELECT * APPLY(parseDateTime64BestEffortOrNull) FROM (SELECT a3 AS s1, \x272020-02-DD\x27 AS s2 FROM (SELECT \x272020-01-DD\x27 AS x)) FORMAT Null;" > fi_file;
        }
    }'
fi

# Verify expression-stack SCOPE_EXIT cleanup is allocation-free on normal return under fault injection,
# and calibrate post-parse single-CTE analysis time (R1_MS = T_1REF_MS - T_PARSE_MS) once per test host.
if [ ! -s "$CACHE_DIR/calib.env" ]; then
    FI_ERR="$TMP_SQL_DIR/err_fault_inject.txt"
    FI_RC=0
    $CLICKHOUSE_LOCAL --tmp --ignore-error --queries-file "$CACHE_DIR/fault_inject.sql" -- --min_allocation_size_to_throw_on_memory_limit=1 --logger.level=none < /dev/null > "$FI_ERR" 2>&1 || FI_RC=$?
    if [ "$FI_RC" -gt 128 ] || grep -qE 'Terminate called|Aborted|Signal 6|Logical error' "$FI_ERR"; then
        printf 'Unexpected abort during expression-stack fault injection (rc=%s): ' "$FI_RC" >&2
        head -c 400 "$FI_ERR" >&2
        printf '\n' >&2
        exit 1
    fi

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
          AND type != 'QueryStart'")

    T_PARSE_MS=${T_PARSE_MS:-5}
    T_1REF_MS=${T_1REF_MS:-100}
    R1_MS=$((T_1REF_MS - T_PARSE_MS))
    if [ "$R1_MS" -lt 30 ]; then
        R1_MS=30
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
            if (t < 0.010) t = 0.010;
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

        if [ "$rc" -eq 0 ] || grep -q 'UNKNOWN_IDENTIFIER' "$err_file"; then
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

run_cancelled_cte "join"       "$CACHE_DIR/join.sql"       "0.10" 0.45 0.30 0.65 0.18 0.85
run_cancelled_cte "expr"       "$CACHE_DIR/expr.sql"       "0.10" 0.45 0.30 0.65 0.18 0.85
run_cancelled_cte "expr_mat"   "$CACHE_DIR/expr_mat.sql"   "0.10" 0.45 0.30 0.65 0.18 0.85
run_cancelled_cte "join_mat_1" "$CACHE_DIR/join_mat_1.sql" "0.10" 0.45 0.30 0.65 0.18 0.85
run_cancelled_cte "join_mat_2" "$CACHE_DIR/join_mat_2.sql" "0.80" 1.35 1.15 1.60 0.95 1.85

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

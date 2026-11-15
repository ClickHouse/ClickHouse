#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Cancellation during CTE resolution must unwind CTE cleanup guards cleanly with
# TIMEOUT_EXCEEDED without recomputing tree hashes or terminating the server process.
# Covers all 5 CTE cleanup sites: join-tree CTE, expression CTE, expression materialized CTE,
# and join-tree materialized CTE (both first and subsequent references).

declare -A CTE_BODY_CACHE
CURRENT_CTE_BODY=""

ensure_cte_body()
{
    local n="$1"
    if [ -z "${CTE_BODY_CACHE[$n]:-}" ]; then
        local tuple_args
        tuple_args=$(seq 1 "$n" | sed 's/.*/number + &/' | paste -sd, -)
        local sum_expr=""
        local j
        for j in $(seq 1 16); do
            if [ -n "$sum_expr" ]; then
                sum_expr="${sum_expr} + "
            fi
            sum_expr="${sum_expr}randConstant(tuple(${j}, ${tuple_args}))"
        done
        CTE_BODY_CACHE[$n]="SELECT toUInt64(${sum_expr}) AS v FROM numbers(0)"
    fi
    CURRENT_CTE_BODY="${CTE_BODY_CACHE[$n]}"
}

WINNING_QIDS=()

run_cancelled_cte()
{
    local label="$1"
    local outer_query_fmt="$2"
    local attempt=0

    for n in 4000 10000 25000 60000; do
        attempt=$((attempt + 1))
        ensure_cte_body "$n"
        local sql
        printf -v sql "$outer_query_fmt" "$CURRENT_CTE_BODY"
        local qid="${CLICKHOUSE_DATABASE}_cte_cancel_${label}_${attempt}_$(random_str 10)"

        local out rc=0
        out=$(printf '%s\n' "$sql" | $CLICKHOUSE_CLIENT \
            --query_id "$qid" \
            --enable_analyzer=1 \
            --enable_materialized_cte=1 \
            --max_query_size=30000000 \
            --max_ast_elements=10000000 \
            --max_expanded_ast_elements=10000000 \
            --max_execution_time=0.01 \
            --timeout_overflow_mode='throw' \
            --log_queries=1 2>&1) || rc=$?

        if [ "$rc" -eq 0 ]; then
            continue
        fi

        if printf '%s\n' "$out" | grep -qE 'Connection reset|Logical error|Aborted|NetException|EOF'; then
            printf 'Unexpected crash/disconnect for %s (n=%s): %s\n' "$label" "$n" "$out" >&2
            return 1
        fi

        if ! printf '%s\n' "$out" | grep -q 'TIMEOUT_EXCEEDED'; then
            printf 'Unexpected non-159 error for %s (rc=%s, n=%s): %s\n' "$label" "$rc" "$n" "$out" >&2
            return 1
        fi

        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
        local before_start_cnt
        before_start_cnt=$($CLICKHOUSE_CLIENT -q "
            SELECT count()
            FROM system.query_log
            WHERE current_database = currentDatabase()
              AND query_id = '$qid'
              AND type = 'ExceptionBeforeStart'
              AND exception_code = 159")

        if [ "${before_start_cnt:-0}" -ge 1 ]; then
            echo "TIMEOUT_EXCEEDED"
            WINNING_QIDS+=("'$qid'")
            return 0
        fi
    done

    printf 'Failed to trigger ExceptionBeforeStart TIMEOUT_EXCEEDED across n ladder for %s\n' "$label" >&2
    return 1
}

run_cancelled_cte "join" "WITH c AS (%s) SELECT v FROM c FORMAT Null"
run_cancelled_cte "expr" "WITH c AS (%s) SELECT 1 WHERE toUInt64(0) IN c FORMAT Null"
run_cancelled_cte "expr_mat" "WITH c AS MATERIALIZED (%s) SELECT 1 WHERE toUInt64(0) IN c FORMAT Null"
run_cancelled_cte "join_mat_1" "WITH c AS MATERIALIZED (%s) SELECT v FROM c FORMAT Null"
run_cancelled_cte "join_mat_2" "WITH c AS MATERIALIZED (%s) SELECT v FROM c UNION ALL SELECT v FROM c FORMAT Null"

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

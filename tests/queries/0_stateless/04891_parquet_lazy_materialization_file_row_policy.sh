#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A row policy reaches the reading step through `query_info.row_level_filter` even when the query
# has no `PREWHERE`, and the source evaluates it inside the main read pass. The lazy-materialization
# split must therefore pin the filter's input columns to the main pass instead of deferring them.
#
# Also covers row policy + `WHERE` moved to `PREWHERE` + lazy materialization: the prewhere pushdown
# shrinks the reading step's header (the row-level filter's input is consumed inside the step), and
# the leftover `ExpressionStep` keeps a dangling unused DAG input, which the lazy-materialization
# optimizer must tolerate rather than fail with `Unknown identifier`.
#
# The whole battery is run with the optimization enabled and disabled; the results must match.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/04891_lazy_mat_row_policy_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"

# The file carries only `k`, `a` and `s`; `d` is missing and comes from its `DEFAULT` expression,
# which depends on `a` — the row policy's input column.
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/row_policy.parquet', Parquet)
    SELECT number AS k, number % 10 AS a, concat('val_', toString(number)) AS s
    FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1;
"

QUERIES="
CREATE TABLE t_lazy_row_policy
(
    k UInt64,
    a UInt64,
    s String,
    d UInt64 DEFAULT a * 2
)
ENGINE = File(Parquet, '${DATA_DIR}/row_policy.parquet');
CREATE ROW POLICY policy_04891 ON t_lazy_row_policy USING a != 0 TO ALL;
SELECT '-- the row policy input is pinned to the main pass (only s is lazy)';
SELECT k, s FROM t_lazy_row_policy ORDER BY k LIMIT 3;
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT k, s FROM t_lazy_row_policy ORDER BY k LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
SELECT '-- row policy + WHERE moved to PREWHERE + lazy materialization';
SELECT k, s FROM t_lazy_row_policy WHERE s != 'val_2' ORDER BY k LIMIT 3;
SELECT '-- a defaulted column computed from the policy input';
SELECT k, d FROM t_lazy_row_policy ORDER BY k LIMIT 3;
SELECT '-- aggregates over the policy-filtered table';
SELECT count(), sum(d), sum(a), uniqExact(a) FROM t_lazy_row_policy;
DROP ROW POLICY policy_04891 ON t_lazy_row_policy;
CREATE ROW POLICY policy_04891_count ON t_lazy_row_policy USING 0 TO ALL;
SELECT '-- a column-less row policy disables the count-only fast path';
SELECT count() FROM t_lazy_row_policy;
DROP ROW POLICY policy_04891_count ON t_lazy_row_policy;
DROP TABLE t_lazy_row_policy;

INSERT INTO FUNCTION file('${DATA_DIR}/row_policy_subcolumn.parquet', Parquet)
SELECT
    number AS k,
    CAST(concat('{\"user\":{\"name\":\"u', toString(number % 2), '\",\"age\":', toString(number), '}}'), 'JSON') AS j
FROM numbers(10)
SETTINGS engine_file_truncate_on_insert = 1;
CREATE TABLE t_lazy_row_policy_subcolumn
(
    k UInt64,
    j JSON
)
ENGINE = File(Parquet, '${DATA_DIR}/row_policy_subcolumn.parquet');
CREATE ROW POLICY policy_04891_subcolumn ON t_lazy_row_policy_subcolumn USING j.user.name != 'u0' TO ALL;
SELECT '-- a row-policy subcolumn keeps its JSON parent in the main pass';
SELECT j.user.age FROM t_lazy_row_policy_subcolumn ORDER BY k LIMIT 3;
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT j.user.age FROM t_lazy_row_policy_subcolumn ORDER BY k LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
DROP ROW POLICY policy_04891_subcolumn ON t_lazy_row_policy_subcolumn;
DROP TABLE t_lazy_row_policy_subcolumn;
"

# `enable_analyzer` is pinned because lazy materialization requires the analyzer
# (see `QueryPlanOptimizationSettings`), and some CI configurations run with the old analyzer.
for enabled in 1 0; do
    echo "-- query_plan_optimize_lazy_materialization_for_file = $enabled"
    "${LOCAL[@]}" \
        --enable_analyzer=1 \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_file="$enabled" \
        --query "$QUERIES"
done

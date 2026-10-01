#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A subcolumn of a defaulted column (`j.user.name` of `j JSON DEFAULT ...`) must take part in the
# default-dependency analysis of lazy materialization through its storage parent: `column_defaults`
# and the identifiers inside default expressions are storage-level names, so matching the
# subcolumn's own name against them would silently split the parent's expression away from its
# inputs, and `AddingDefaultsTransform` (which runs inside every branch of the read pipeline) would
# compute the parent from type defaults instead of the row's real values.
#
# The whole battery is run with the optimization enabled and disabled; the results must match.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/04869_lazy_mat_default_subcolumn_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"

# The file carries only `k`, `a` and `s`; `j` is missing and comes from its `DEFAULT` expression,
# which depends on `a`.
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/defaults.parquet', Parquet)
    SELECT number AS k, 999 - number AS a, concat('val_', toString(number)) AS s
    FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1;
"

QUERIES="
CREATE TABLE t_lazy_default_subcolumn
(
    k UInt64,
    a UInt64,
    s String,
    j JSON DEFAULT toJSONString(map('user', map('name', concat('u', toString(a)))))
)
ENGINE = File(Parquet, '${DATA_DIR}/defaults.parquet');
SELECT '-- a subcolumn of a defaulted parent is pinned when the expression input is a sort key';
SELECT j.user.name, s FROM t_lazy_default_subcolumn ORDER BY a DESC LIMIT 3;
SELECT countIf(explain LIKE '%LazilyReadFromFile%') FROM (EXPLAIN SELECT j.user.name, s FROM t_lazy_default_subcolumn ORDER BY a DESC LIMIT 3);
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT j.user.name, s FROM t_lazy_default_subcolumn ORDER BY a DESC LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
SELECT '-- a subcolumn of a defaulted parent is deferred together with the expression input';
SELECT a, j.user.name FROM t_lazy_default_subcolumn ORDER BY k LIMIT 3;
SELECT countIf(explain LIKE '%LazilyReadFromFile%') FROM (EXPLAIN SELECT a, j.user.name FROM t_lazy_default_subcolumn ORDER BY k LIMIT 3);
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT a, j.user.name FROM t_lazy_default_subcolumn ORDER BY k LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
SELECT '-- sorting by a subcolumn of a defaulted parent pins the expression input';
SELECT a, s FROM t_lazy_default_subcolumn ORDER BY j.user.name.:String DESC LIMIT 3;
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT a, s FROM t_lazy_default_subcolumn ORDER BY j.user.name.:String DESC LIMIT 3) WHERE explain LIKE '%Lazily read columns%';
DROP TABLE t_lazy_default_subcolumn;
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

#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A row policy on a `File` table must not remove its input columns from the output header of
# `ReadFromFile` when a `WHERE` condition is moved to `PREWHERE`. Before the fix, a `DEFAULT`
# column computed from the policy's input column failed with `UNKNOWN_IDENTIFIER`, and the step
# above the read kept a DAG input that did not exist in the header anymore.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05291_file_row_policy_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"

# The file has only `k`, `a` and `s`. The column `d` comes from its `DEFAULT` expression.
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/data.parquet', Parquet)
    SELECT number AS k, number % 10 AS a, concat('val_', toString(number)) AS s
    FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1;
"

QUERIES="
CREATE TABLE t (k UInt64, a UInt64, s String, d UInt64 DEFAULT a * 2)
ENGINE = File(Parquet, '${DATA_DIR}/data.parquet');
CREATE ROW POLICY policy_05291 ON t USING a != 0 TO ALL;
SELECT '-- DEFAULT column from the policy input, WHERE moved to PREWHERE';
SELECT k, d FROM t WHERE s != 'val_2' ORDER BY k LIMIT 3;
SELECT '-- WHERE on the policy input';
SELECT k, s, d FROM t WHERE a != 1 ORDER BY k LIMIT 3;
SELECT '-- the read header does not depend on PREWHERE';
SELECT countIf(explain LIKE '% a UInt64') FROM (EXPLAIN header = 1 SELECT k, s FROM t WHERE s != 'val_2' SETTINGS optimize_move_to_prewhere = 0);
SELECT countIf(explain LIKE '% a UInt64') FROM (EXPLAIN header = 1 SELECT k, s FROM t WHERE s != 'val_2' SETTINGS optimize_move_to_prewhere = 1);
SELECT '-- aggregates';
SELECT count(), sum(d) FROM t WHERE s != 'val_2';
DROP ROW POLICY policy_05291 ON t;
DROP TABLE t;
"

for enabled in 1 0; do
    echo "-- query_plan_optimize_lazy_materialization_for_file = $enabled"
    "${LOCAL[@]}" \
        --enable_analyzer=1 \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_file="$enabled" \
        --query "$QUERIES"
done

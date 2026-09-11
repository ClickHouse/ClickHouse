#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: Depends on Parquet

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Filter / row policy on a DEFAULT column missing from the Parquet file is deferred until after
# AddingDefaultsTransform. With query_plan_optimize_lazy_materialization_for_file, a later column
# (here `s`) is re-read via ChunkInfoRowNumbers. Those filters must update that metadata when they
# drop rows; otherwise the lazy reread joins the wrong physical rows.
#
# Run with the optimization enabled and disabled; results must match.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05029_lazy_mat_default_filter_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/defaults.parquet', Parquet)
    SELECT number AS k, number % 10 AS a, concat('val_', toString(number)) AS s
    FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1;
"

QUERIES="
DROP TABLE IF EXISTS t_lazy_default_filter;
DROP ROW POLICY IF EXISTS pol_d ON t_lazy_default_filter;

CREATE TABLE t_lazy_default_filter
(
    k UInt64,
    a UInt64,
    s String,
    d UInt64 DEFAULT a * 2
)
ENGINE = File(Parquet, '${DATA_DIR}/defaults.parquet');

CREATE ROW POLICY pol_d ON t_lazy_default_filter USING d != 0 TO ALL;

SELECT '-- row policy on missing DEFAULT; lazily read s after filtering';
SELECT k, s FROM t_lazy_default_filter ORDER BY k LIMIT 3;
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT k, s FROM t_lazy_default_filter ORDER BY k LIMIT 3) WHERE explain LIKE '%Lazily read columns%';

-- File rejects PREWHERE on a DEFAULT column missing from the file (ILLEGAL_PREWHERE).
-- WHERE still filters on the computed default after AddingDefaultsTransform.
SELECT '-- WHERE on missing DEFAULT; lazily read s';
SELECT k, s FROM t_lazy_default_filter WHERE d > 2 ORDER BY k LIMIT 3;

SELECT '-- values of the defaulted filter column after deferred filter';
SELECT k, d FROM t_lazy_default_filter ORDER BY k LIMIT 3;

DROP ROW POLICY pol_d ON t_lazy_default_filter;
DROP TABLE t_lazy_default_filter;
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

#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `StorageView::canHideRows` classifies the stored view AST before SQL user-defined functions are
# expanded (`UserDefinedSQLFunctionVisitor` on the legacy path, `resolveFunction` on the analyzer
# path). A `SQL SECURITY DEFINER` / `NONE` view whose only row-hiding construct sits behind a UDF
# wrapper - `CREATE FUNCTION f AS (a) -> arrayJoin(a)` - used to be classified as projection-only
# and stayed on the fast path, where the invoker's predicate is evaluated on rows the view hides.
# The classifier now descends into SQL UDF bodies, recursively.
#
# SQL user-defined functions are server-global, so their names carry the test database: concurrent
# runs of this test would otherwise drop each other's functions.

db=${CLICKHOUSE_DATABASE}
f_array_join="f05082_${db}_array_join"
f_nested="f05082_${db}_nested"
f_sum="f05082_${db}_sum"
f_plain="f05082_${db}_plain"

CLIENT="${CLICKHOUSE_CLIENT} --query_plan_lift_up_array_join 1 --query_plan_filter_push_down 1 --query_plan_merge_expressions 1 --enable_parallel_replicas 0 --max_threads 1"

$CLIENT --query "DROP FUNCTION IF EXISTS $f_array_join"
$CLIENT --query "DROP FUNCTION IF EXISTS $f_nested"
$CLIENT --query "DROP FUNCTION IF EXISTS $f_sum"
$CLIENT --query "DROP FUNCTION IF EXISTS $f_plain"

$CLIENT <<EOF
CREATE TABLE $db.t05082 (key UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY key;
INSERT INTO $db.t05082 SELECT number, if(number = 42, [], [number, number + 1]) FROM numbers(100);

CREATE FUNCTION $f_array_join AS (a) -> arrayJoin(a);
CREATE FUNCTION $f_nested AS (a) -> $f_array_join(a) + 0;
CREATE FUNCTION $f_sum AS (x) -> sum(x);
CREATE FUNCTION $f_plain AS (x) -> x + 1;

CREATE VIEW $db.v05082_invoker SQL SECURITY INVOKER AS
    SELECT key, $f_array_join(arr) AS item FROM $db.t05082;
CREATE VIEW $db.v05082_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT key, $f_array_join(arr) AS item FROM $db.t05082;
CREATE VIEW $db.v05082_none SQL SECURITY NONE AS
    SELECT key, $f_array_join(arr) AS item FROM $db.t05082;
CREATE VIEW $db.v05082_nested_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT key, $f_nested(arr) AS item FROM $db.t05082;

-- The sibling carrier from the same root cause: a UDF that expands to an aggregate without
-- GROUP BY collapses the rows. An outer predicate never moves below an aggregation, so there is
-- no pushdown oracle; the classification is observable through analyzer_inline_views instead.
-- A barrier view that provably hides no rows is inlined and disappears from the query tree, while
-- one that hides rows stays a table expression.
CREATE VIEW $db.v05082_plain_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT $f_plain(key) AS k FROM $db.t05082;
CREATE VIEW $db.v05082_sum_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT $f_sum(key) AS s FROM $db.t05082;
EOF

for analyzer in 1 0; do
    if [ "$analyzer" = 1 ]; then label="analyzer"; else label="legacy analyzer"; fi

    # The INVOKER view stays fully optimizable, so the outer predicate reaches the row that the
    # empty array hides. This is the positive control proving that the oracle discriminates.
    echo "invoker, $label: $($CLIENT --enable_analyzer "$analyzer" --query "SELECT count() FROM $db.v05082_invoker WHERE throwIf(key = 42, 'DISCLOSED') = 0" 2>&1 | grep -q -F FUNCTION_THROW_IF_VALUE_IS_NON_ZERO && echo 1 || echo 0)"

    # The barrier views keep the predicate above the row-dropping arrayJoin hidden in the UDF.
    $CLIENT --enable_analyzer "$analyzer" --query "SELECT 'definer, $label:', count() FROM $db.v05082_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0"
    $CLIENT --enable_analyzer "$analyzer" --query "SELECT 'none, $label:', count() FROM $db.v05082_none WHERE throwIf(key = 42, 'DISCLOSED') = 0"
    $CLIENT --enable_analyzer "$analyzer" --query "SELECT 'nested udf definer, $label:', count() FROM $db.v05082_nested_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0"

    if [ "$analyzer" = 1 ]; then
        $CLIENT --enable_analyzer 1 --analyzer_inline_views 1 --query "SELECT 'definer, analyzer, inline views:', count() FROM $db.v05082_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0"
        $CLIENT --enable_analyzer 1 --analyzer_inline_views 1 --query "SELECT 'nested udf definer, analyzer, inline views:', count() FROM $db.v05082_nested_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0"
    fi
done

# The barrier only drops the optimization, never the result.
$CLIENT --enable_analyzer 1 --query "SELECT 'definer results:', count(), min(key), max(key) FROM $db.v05082_definer WHERE key % 2 = 0"

$CLIENT --enable_analyzer 1 --analyzer_inline_views 1 --query "SELECT 'plain udf definer kept as a table expression:', countIf(explain LIKE '%table_name: %v05082_plain_definer%') FROM (EXPLAIN QUERY TREE SELECT k FROM $db.v05082_plain_definer)"
$CLIENT --enable_analyzer 1 --analyzer_inline_views 1 --query "SELECT 'sum udf definer kept as a table expression:', countIf(explain LIKE '%table_name: %v05082_sum_definer%') FROM (EXPLAIN QUERY TREE SELECT s FROM $db.v05082_sum_definer)"
$CLIENT --enable_analyzer 1 --analyzer_inline_views 1 --query "SELECT 'sum udf definer result:', s FROM $db.v05082_sum_definer"

$CLIENT <<EOF
DROP VIEW $db.v05082_invoker;
DROP VIEW $db.v05082_definer;
DROP VIEW $db.v05082_none;
DROP VIEW $db.v05082_nested_definer;
DROP VIEW $db.v05082_plain_definer;
DROP VIEW $db.v05082_sum_definer;
DROP FUNCTION $f_array_join;
DROP FUNCTION $f_nested;
DROP FUNCTION $f_sum;
DROP FUNCTION $f_plain;
DROP TABLE $db.t05082;
EOF

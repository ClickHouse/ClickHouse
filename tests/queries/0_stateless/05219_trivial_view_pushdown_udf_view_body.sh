#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A view body whose aggregate, window function or subquery sits behind a SQL user-defined function
# - `CREATE FUNCTION f AS (x) -> sum(x)` - is not a trivial projection, so
# `optimize_trivial_view_pushdown_to_distributed` must not ship the whole outer query to the shards:
# each shard would aggregate its own rows and the initiator would concatenate the per-shard results
# (`SELECT count() FROM v` over a two-shard cluster would return 2 instead of 1). `CREATE VIEW`
# stores the view with its SQL UDFs already substituted, and the classifiers of
# `tryGetTrivialViewUnderlyingStorage` descend into SQL UDF bodies as defense in depth; this test
# pins the end-to-end guarantee whichever layer provides it, next to 04837 which covers the same
# view bodies spelled without a UDF.
#
# SQL user-defined functions are server-global, so their names carry the test database: concurrent
# runs of this test would otherwise drop each other's functions.

db=${CLICKHOUSE_DATABASE}
f_plain="f05219_${db}_plain"
f_sum="f05219_${db}_sum"
f_nested="f05219_${db}_nested"
f_window="f05219_${db}_window"
f_scalar="f05219_${db}_scalar"
f_in="f05219_${db}_in"

# The pushdown oracle greps for the "Convert VIEW subquery result to VIEW table structure" step,
# which only the legacy EXPLAIN format prints.
CLIENT="${CLICKHOUSE_CLIENT} --enable_analyzer 1 --explain_query_plan_default legacy --prefer_localhost_replica 0 --optimize_trivial_view_pushdown_to_distributed 1"

for f in "$f_plain" "$f_sum" "$f_nested" "$f_window" "$f_scalar" "$f_in"; do
    $CLIENT --query "DROP FUNCTION IF EXISTS $f"
done

$CLIENT --queries-file /dev/stdin <<EOF2
CREATE TABLE $db.t05219_local (id UInt32, v UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE $db.t05219_dist AS $db.t05219_local
    ENGINE = Distributed(test_cluster_two_shards, '$db', t05219_local);
CREATE TABLE $db.t05219_other (m UInt32) ENGINE = MergeTree ORDER BY m;

INSERT INTO $db.t05219_local VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO $db.t05219_other VALUES (2), (100);

CREATE FUNCTION $f_plain AS (x) -> x + 1;
CREATE FUNCTION $f_sum AS (x) -> sum(x);
CREATE FUNCTION $f_nested AS (x) -> $f_sum(x) + 0;
CREATE FUNCTION $f_window AS (x, k) -> sum(x) OVER (ORDER BY k);
CREATE FUNCTION $f_scalar AS () -> (SELECT max(m) FROM $db.t05219_other);
CREATE FUNCTION $f_in AS (x) -> x IN (SELECT m FROM $db.t05219_other);

-- Positive control: a UDF that expands to a plain expression keeps the view trivial.
CREATE VIEW $db.v05219_plain AS SELECT id, $f_plain(v) AS w FROM $db.t05219_dist;
-- The view body aggregates behind a UDF, directly and through a nested UDF.
CREATE VIEW $db.v05219_sum AS SELECT $f_sum(v) AS s FROM $db.t05219_dist;
CREATE VIEW $db.v05219_nested AS SELECT $f_nested(v) AS s FROM $db.t05219_dist;
-- The view body has a window function behind a UDF.
CREATE VIEW $db.v05219_window AS SELECT id, $f_window(v, id) AS running_sum FROM $db.t05219_dist;
-- The view body has a scalar subquery behind a UDF.
CREATE VIEW $db.v05219_scalar AS SELECT id, $f_scalar() AS x FROM $db.t05219_dist;
-- The view body's WHERE has a subquery behind a UDF.
CREATE VIEW $db.v05219_in AS SELECT id, v FROM $db.t05219_dist WHERE $f_in(id);
EOF2

echo "plain udf, pushdown fires: $($CLIENT --query "SELECT countIf(explain LIKE '%VIEW subquery%') = 0 FROM (EXPLAIN SELECT id, w FROM $db.v05219_plain)")"
for view in sum nested window scalar in; do
    echo "$view udf, pushdown suppressed: $($CLIENT --query "SELECT countIf(explain LIKE '%VIEW subquery%') > 0 FROM (EXPLAIN SELECT * FROM $db.v05219_$view)")"
done

# The result must not depend on the setting. Every row of the local table is visible to both shards
# of `test_cluster_two_shards`, so a per-shard aggregation would produce two rows instead of one.
for pushdown in 1 0; do
    $CLIENT --optimize_trivial_view_pushdown_to_distributed "$pushdown" --query "SELECT 'sum udf, pushdown $pushdown:', count(), sum(s) FROM $db.v05219_sum"
    $CLIENT --optimize_trivial_view_pushdown_to_distributed "$pushdown" --query "SELECT 'nested udf, pushdown $pushdown:', count(), sum(s) FROM $db.v05219_nested"
    $CLIENT --optimize_trivial_view_pushdown_to_distributed "$pushdown" --query "SELECT 'window udf, pushdown $pushdown:', id, running_sum FROM $db.v05219_window ORDER BY id, running_sum"
    $CLIENT --optimize_trivial_view_pushdown_to_distributed "$pushdown" --query "SELECT 'scalar udf, pushdown $pushdown:', count(), max(x) FROM $db.v05219_scalar"
    $CLIENT --optimize_trivial_view_pushdown_to_distributed "$pushdown" --query "SELECT 'in udf, pushdown $pushdown:', count(), sum(v) FROM $db.v05219_in"
done

for f in "$f_plain" "$f_sum" "$f_nested" "$f_window" "$f_scalar" "$f_in"; do
    $CLIENT --query "DROP FUNCTION $f"
done

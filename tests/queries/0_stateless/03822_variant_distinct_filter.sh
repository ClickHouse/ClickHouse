#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings
# Regression test for ColumnVariant::filter/permute/index optimization paths.
# The optimization for "one non-empty variant, no NULLs" must not carry over
# non-active variant columns that could have inconsistent sizes.
#
# The bug requires concurrent queries on the same Memory table: multiple
# connections share ColumnVariant blocks via COW, and the permute optimization
# used assumeMutable() on non-active variants, corrupting shared columns.
# https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=4d4a583a5ad2322918638a3f6a01acd7e0ed7019&name_0=MasterCI&name_1=AST%20fuzzer%20%28amd_tsan%29

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --allow_suspicious_types_in_order_by=1 --use_variant_default_implementation_for_comparisons=0 --max_execution_time=10"

$CH_CLIENT -q "DROP TABLE IF EXISTS test_variant_distinct"
$CH_CLIENT -q "CREATE TABLE test_variant_distinct (v1 Variant(String, UInt64, Array(UInt32)), v2 Variant(String, UInt64, Array(UInt32))) ENGINE = Memory"

# Each INSERT creates a separate block in the Memory engine.
# Concurrent queries will share these blocks via COW pointers.
inserts=""
for i in $(seq 1 10); do
    inserts+="INSERT INTO test_variant_distinct VALUES ($i, $i);"
    inserts+="INSERT INTO test_variant_distinct VALUES ('s_$i', 's_$i');"
    inserts+="INSERT INTO test_variant_distinct VALUES ([1,2,$i], [1,2,$i]);"
    inserts+="INSERT INTO test_variant_distinct VALUES (NULL, NULL);"
    inserts+="INSERT INTO test_variant_distinct VALUES ($i, 's_$i');"
    inserts+="INSERT INTO test_variant_distinct VALUES ('s_$i', [1,2,$i]);"
    inserts+="INSERT INTO test_variant_distinct VALUES ([1,2,$i], NULL);"
    inserts+="INSERT INTO test_variant_distinct VALUES (NULL, $i);"
done
echo "$inserts" | $CH_CLIENT -n

# Run concurrent queries from multiple connections to trigger the race.
# The permute optimization (ORDER BY) corrupts shared non-active variant
# columns via assumeMutable(), then filter (DISTINCT) reads the corrupted data.
function run_queries()
{
    local TIMELIMIT=$((SECONDS + 5))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CH_CLIENT -q "SELECT v2 FROM test_variant_distinct WHERE toLowCardinality(1) ORDER BY v2 ASC NULLS FIRST FORMAT Null" 2>/dev/null
        $CH_CLIENT -q "SELECT DISTINCT v2 FROM test_variant_distinct WHERE toLowCardinality(1) ORDER BY v2 ASC NULLS FIRST FORMAT Null" 2>/dev/null
        $CH_CLIENT -q "SELECT v2 FROM test_variant_distinct WHERE toLowCardinality(1) ORDER BY v2 DESC NULLS LAST FORMAT Null" 2>/dev/null
        $CH_CLIENT -q "SELECT DISTINCT v2 FROM test_variant_distinct WHERE toLowCardinality(1) ORDER BY v2 DESC FORMAT Null" 2>/dev/null
    done
}

export -f run_queries
export CH_CLIENT

# Launch several concurrent query loops to create COW sharing contention.
for _ in $(seq 1 4); do
    run_queries &
done

wait

$CH_CLIENT -q "DROP TABLE test_variant_distinct"
echo "OK"

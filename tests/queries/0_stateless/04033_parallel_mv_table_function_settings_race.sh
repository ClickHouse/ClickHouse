#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database
# Tag no-fasttest: uses multiple materialized views with parallel processing

# Test for TSan data race in QueryAnalyzer::resolveQuery where
# parallel_replicas_for_cluster_engines is set on the shared query context
# while another thread reads settings via executeTableFunction.
#
# The race requires:
# 1. Multiple materialized views processed in parallel (parallel_view_processing=1)
# 2. Each MV uses a table function (triggers resolveTableFunction -> executeTableFunction -> getSettingsRef().changes())
# 3. The MV's query structure causes shouldReplaceWithClusterAlternatives()=false
#    (triggers setSetting on the context in resolveQuery)
# A CROSS JOIN with a table function satisfies both conditions 2 and 3.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mv1;
    DROP TABLE IF EXISTS mv2;
    DROP TABLE IF EXISTS mv3;
    DROP TABLE IF EXISTS dest1;
    DROP TABLE IF EXISTS dest2;
    DROP TABLE IF EXISTS dest3;
    DROP TABLE IF EXISTS source;

    CREATE TABLE source (x UInt64) ENGINE = Null;
    CREATE TABLE dest1 (x UInt64, y UInt64) ENGINE = Null;
    CREATE TABLE dest2 (x UInt64, y UInt64) ENGINE = Null;
    CREATE TABLE dest3 (x UInt64, y UInt64) ENGINE = Null;

    -- Each MV uses CROSS JOIN with a table function.
    -- This makes shouldReplaceWithClusterAlternatives() return false
    -- (table_count=1, table_function_count=1 -> sum != 1 && tf_count != 0)
    -- and triggers resolveTableFunction which reads settings via changes().
    CREATE MATERIALIZED VIEW mv1 TO dest1 AS
    SELECT source.x, n.number AS y FROM source CROSS JOIN numbers(10) AS n;

    CREATE MATERIALIZED VIEW mv2 TO dest2 AS
    SELECT source.x, n.number AS y FROM source CROSS JOIN numbers(10) AS n;

    CREATE MATERIALIZED VIEW mv3 TO dest3 AS
    SELECT source.x, n.number AS y FROM source CROSS JOIN numbers(10) AS n;
"

# Run multiple iterations to increase the chance of triggering the race under TSan.
for _ in $(seq 1 5); do
    ${CLICKHOUSE_CLIENT} --query "
        SET parallel_view_processing = 1;
        SET max_threads = 8, max_insert_threads = 8;
        SET max_block_size = 100, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
        INSERT INTO source SELECT number FROM numbers(1000);
    "
done

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mv1;
    DROP TABLE mv2;
    DROP TABLE mv3;
    DROP TABLE dest1;
    DROP TABLE dest2;
    DROP TABLE dest3;
    DROP TABLE source;
"

echo "OK"

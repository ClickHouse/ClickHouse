#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: `CREATE CLONE AS is not supported with Replicated databases`, `ON CLUSTER is not allowed for Replicated database`
# A clone copies its definition out of stored metadata and an `ON CLUSTER` worker sees only the
# query text, so neither can recover the spelling from the issuing session's settings.
# The UDF name includes the test database because SQL UDFs are server-global and this test runs
# concurrently with itself in flaky-check mode.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

UDF="totime_04909_clone_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 1 -q "CREATE FUNCTION ${UDF} AS x -> toTime(x);"

# A clone must retain its source definition verbatim. Unlike plain AS, changing its key spelling
# makes the subsequent partition copy reject the source and destination as structurally different.
${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 -q "
CREATE TABLE t_clone_source (c0 DateTime) ENGINE = MergeTree() ORDER BY toTime(c0);
"
${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 1 --multiquery -q "
CREATE TABLE t_clone_key CLONE AS t_clone_source;
SELECT 'clone', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_clone_key';
"

# The oldest DDL entry format ships the query text as written and carries no settings, so the
# spelling a worker receives has to be unambiguous already.
OLDEST_VERSION=1
${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 1 \
    --distributed_ddl_entry_format_version $OLDEST_VERSION --distributed_ddl_output_mode none --multiquery -q "
CREATE TABLE t_on_cluster_key ON CLUSTER test_shard_localhost (c0 DateTime) ENGINE = MergeTree() ORDER BY toTime(c0);
SELECT 'on_cluster_oldest_entry', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_on_cluster_key';

-- The same, with the key expression carried by a SQL UDF body.
CREATE TABLE t_on_cluster_udf_key ON CLUSTER test_shard_localhost (c0 DateTime) ENGINE = MergeTree() ORDER BY ${UDF}(c0);
SELECT 'on_cluster_oldest_entry_udf', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_on_cluster_udf_key';

-- A materialized view keeps its inner table's key outside the top-level storage definition.
CREATE TABLE ${CLICKHOUSE_DATABASE}.t_on_cluster_mv_src (c0 DateTime) ENGINE = MergeTree() ORDER BY tuple();
CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.t_on_cluster_mv ON CLUSTER test_shard_localhost
ENGINE = MergeTree() ORDER BY toTime(c0) AS SELECT c0 FROM ${CLICKHOUSE_DATABASE}.t_on_cluster_mv_src;
SELECT 'on_cluster_oldest_entry_mv', extract(create_table_query, 'ORDER BY [^ ]*') FROM system.tables WHERE database = currentDatabase() AND name = 't_on_cluster_mv';

-- A projection carries its own physical sorting key, declared inside the column list.
CREATE TABLE ${CLICKHOUSE_DATABASE}.t_on_cluster_projection_key ON CLUSTER test_shard_localhost
(c0 DateTime, v UInt32, PROJECTION pr (SELECT c0, v ORDER BY toTime(c0))) ENGINE = MergeTree() ORDER BY tuple();
SELECT 'on_cluster_oldest_entry_projection', extract(create_table_query, 'ORDER BY toTime[A-Za-z]*') FROM system.tables WHERE database = currentDatabase() AND name = 't_on_cluster_projection_key';
"

${CLICKHOUSE_CLIENT} --multiquery -q "
DROP TABLE t_clone_key;
DROP TABLE t_clone_source;
DROP TABLE t_on_cluster_key;
DROP TABLE t_on_cluster_udf_key;
DROP TABLE t_on_cluster_mv;
DROP TABLE t_on_cluster_projection_key;
DROP TABLE t_on_cluster_mv_src;
DROP FUNCTION ${UDF};
"

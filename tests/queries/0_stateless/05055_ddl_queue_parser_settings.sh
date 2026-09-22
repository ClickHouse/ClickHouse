#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# Tag no-replicated-database: ON CLUSTER is not allowed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE="ddl_queue_parser_settings_05055"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
        --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${TABLE} ON CLUSTER test_shard_localhost" >/dev/null 2>&1 || true
}

cleanup
trap cleanup EXIT

${CLICKHOUSE_CLIENT} \
    --dialect=clickhouse \
    --allow_experimental_kusto_dialect=1 \
    --allow_experimental_prql_dialect=1 \
    --allow_experimental_polyglot_dialect=1 \
    --polyglot_dialect=mysql \
    --enable_json_ast_dialect=1 \
    --enable_trino_dialect=1 \
    --promql_database=promql_database_from_context \
    --promql_table=promql_table_from_context \
    --promql_evaluation_time=42 \
    --enable_time_series_table=1 \
    --join_use_nulls=1 \
    --distributed_ddl_output_mode=none \
    --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE} ON CLUSTER test_shard_localhost
        ENGINE = Memory AS SELECT 1
        SETTINGS
            dialect = 'clickhouse',
            allow_experimental_kusto_dialect = 1,
            allow_experimental_prql_dialect = 1,
            allow_experimental_polyglot_dialect = 1,
            polyglot_dialect = 'mysql',
            enable_json_ast_dialect = 1,
            enable_trino_dialect = 1,
            promql_database = 'promql_database_from_query',
            promql_table = 'promql_table_from_query',
            evaluation_time = 43,
            enable_time_series_table = 1,
            join_use_nulls = 1"

${CLICKHOUSE_CLIENT} --query "
    WITH
        [
            'dialect',
            'allow_experimental_kusto_dialect',
            'allow_experimental_prql_dialect',
            'allow_experimental_polyglot_dialect',
            'polyglot_dialect',
            'enable_json_ast_dialect',
            'enable_trino_dialect',
            'promql_database',
            'promql_table',
            'promql_evaluation_time',
            'evaluation_time'
        ] AS parser_settings,
        [
            'join_use_nulls',
            'enable_time_series_table'
        ] AS semantic_settings
    SELECT
        count() = 1,
        countIf(NOT arrayExists(name -> mapContains(settings, name), parser_settings)) = count(),
        countIf(arrayAll(name -> mapContains(settings, name), semantic_settings)) = count(),
        countIf(NOT arrayExists(name -> position(query, name) > 0, parser_settings)) = count(),
        countIf(arrayAll(name -> position(query, name) > 0, semantic_settings)) = count()
    FROM system.distributed_ddl_queue
    WHERE position(query, '${CLICKHOUSE_DATABASE}.${TABLE}') > 0
        AND position(query, 'CREATE TABLE') > 0
"

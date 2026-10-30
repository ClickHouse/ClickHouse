#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# Tag no-replicated-database: ON CLUSTER is not allowed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE="ddl_worker_parser_settings_on_cluster_05055"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
        --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${TABLE} ON CLUSTER test_shard_localhost" >/dev/null 2>&1 || true
    for setting_source in query session; do
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.ddl_parser_local_${setting_source} SYNC" >/dev/null 2>&1 || true
        ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
            --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.ddl_parser_cluster_${setting_source} ON CLUSTER test_shard_localhost SYNC" >/dev/null 2>&1 || true
    done
}

cleanup
trap cleanup EXIT

# A non-default `async_insert_select_as_async_insert` must be stripped even when set only in the session.
${CLICKHOUSE_CLIENT} \
    --dialect=clickhouse \
    --allow_experimental_kusto_dialect=1 \
    --allow_experimental_prql_dialect=1 \
    --allow_experimental_polyglot_dialect=1 \
    --polyglot_dialect=mysql \
    --enable_json_ast_dialect=1 \
    --enable_logsql_dialect=1 \
    --logsql_database=logsql_database_from_context \
    --logsql_table=logsql_table_from_context \
    --logsql_time_column=timestamp_from_context \
    --logsql_message_column=message_from_context \
    --enable_trino_dialect=1 \
    --implicit_select=1 \
    --allow_settings_after_format_in_insert=1 \
    --promql_database=promql_database_from_context \
    --promql_table=promql_table_from_context \
    --promql_evaluation_time=42 \
    --enable_time_series_table=1 \
    --async_insert_select_as_async_insert=0 \
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
            enable_logsql_dialect = 1,
            logsql_database = 'logsql_database_from_query',
            logsql_table = 'logsql_table_from_query',
            logsql_time_column = 'timestamp_from_query',
            logsql_message_column = 'message_from_query',
            enable_trino_dialect = 1,
            implicit_select = 1,
            allow_settings_after_format_in_insert = 1,
            promql_database = 'promql_database_from_query',
            promql_table = 'promql_table_from_query',
            evaluation_time = 43,
            enable_time_series_table = 1,
            join_use_nulls = 1"

echo "queued settings"
${CLICKHOUSE_CLIENT} --query "
    WITH
        [
            'dialect',
            'allow_experimental_kusto_dialect',
            'allow_experimental_prql_dialect',
            'allow_experimental_polyglot_dialect',
            'polyglot_dialect',
            'enable_json_ast_dialect',
            'enable_logsql_dialect',
            'logsql_database',
            'logsql_table',
            'logsql_time_column',
            'logsql_message_column',
            'enable_trino_dialect',
            'promql_database',
            'promql_table',
            'promql_evaluation_time',
            'evaluation_time'
        ] AS parser_settings,
        [
            'implicit_select',
            'allow_settings_after_format_in_insert',
            'join_use_nulls',
            'enable_time_series_table'
        ] AS semantic_settings
    SELECT
        count() = 1,
        countIf(NOT arrayExists(name -> mapContains(settings, name), parser_settings)) = count(),
        countIf(arrayAll(name -> mapContains(settings, name), semantic_settings)) = count(),
        countIf(NOT arrayExists(name -> position(query, name) > 0, parser_settings)) = count(),
        countIf(arrayAll(name -> position(query, name) > 0, semantic_settings)) = count(),
        countIf(mapContains(settings, 'async_insert_select_as_async_insert')) = 0,
        countIf(settings['join_use_nulls'] = '1') = count()
    FROM system.distributed_ddl_queue
    WHERE position(query, '${CLICKHOUSE_DATABASE}.${TABLE}') > 0
        AND position(query, 'CREATE TABLE') > 0
"

for setting_source in query session; do
    settings_clause=""
    session_value=1
    if [[ ${setting_source} == query ]]; then
        session_value=0
        settings_clause="SETTINGS implicit_select = 1, allow_settings_after_format_in_insert = 1"
    fi

    for execution in local cluster; do
        on_cluster=""
        if [[ ${execution} == cluster ]]; then
            on_cluster="ON CLUSTER test_shard_localhost"
        fi
        table="${CLICKHOUSE_DATABASE}.ddl_parser_${execution}_${setting_source}"

        ${CLICKHOUSE_CLIENT} --implicit_select="${session_value}" --allow_settings_after_format_in_insert="${session_value}" \
            --distributed_ddl_output_mode=throw --query "
                CREATE TABLE ${table} ${on_cluster} ENGINE = Memory AS SELECT
                    formatQueryOrNull('1 + 1') AS formatted,
                    JSONHas(parseQueryToJSON('INSERT INTO t SELECT 1 FORMAT Null SETTINGS max_threads = 1'), 'settings_ast') AS parsed_settings
                ${settings_clause}" >/dev/null
        ${CLICKHOUSE_CLIENT} --query "SELECT '${setting_source}', '${execution}', formatted, parsed_settings FROM ${table}"
    done
done

#!/usr/bin/env bash
# Tags: replica

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

DB="ddl_worker_parser_settings_rdb_05055_${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE="ddl_worker_parser_settings_replicated_database_05055"
ZK_PATH="/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${DB}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB} SYNC" >/dev/null 2>&1 || true
    ${CLICKHOUSE_KEEPER_CLIENT} --query "rmr ${ZK_PATH}" >/dev/null 2>&1 || true
}

cleanup
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query \
    "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', '{shard}', '{replica}')"

${CLICKHOUSE_CLIENT} \
    --distributed_ddl_output_mode=none \
    --enable_json_ast_dialect=1 \
    --enable_logsql_dialect=1 \
    --logsql_database=logsql_database_from_context \
    --logsql_table=logsql_table_from_context \
    --logsql_time_column=timestamp_from_context \
    --logsql_message_column=message_from_context \
    --implicit_select=1 \
    --allow_settings_after_format_in_insert=1 \
    --join_use_nulls=1 \
    --query "CREATE TABLE ${DB}.${TABLE} ENGINE = Memory AS SELECT 1 AS value
        SETTINGS
            enable_json_ast_dialect = 1,
            enable_logsql_dialect = 1,
            logsql_database = 'logsql_database_from_query',
            logsql_table = 'logsql_table_from_query',
            logsql_time_column = 'timestamp_from_query',
            logsql_message_column = 'message_from_query',
            implicit_select = 1,
            allow_settings_after_format_in_insert = 1,
            database = 'system',
            join_use_nulls = 1"

echo "queued settings"
${CLICKHOUSE_CLIENT} --query "
    WITH
        [
            'enable_json_ast_dialect',
            'enable_logsql_dialect',
            'logsql_database',
            'logsql_table',
            'logsql_time_column',
            'logsql_message_column'
        ] AS parser_settings,
        ['database ='] AS initiator_only_settings,
        ['implicit_select', 'allow_settings_after_format_in_insert', 'join_use_nulls'] AS semantic_settings
    SELECT
        count() = 1,
        countIf(NOT arrayExists(setting -> position(value, setting) > 0, parser_settings)) = count(),
        countIf(NOT arrayExists(setting -> position(value, setting) > 0, initiator_only_settings)) = count(),
        countIf(arrayAll(setting -> position(value, setting) > 0, semantic_settings)) = count()
    FROM system.zookeeper
    WHERE path = '${ZK_PATH}/log'
        AND startsWith(name, 'query-')
        AND position(value, '${TABLE}') > 0
"

for setting_source in query session; do
    settings_clause=""
    session_value=1
    if [[ ${setting_source} == query ]]; then
        session_value=0
        settings_clause="SETTINGS implicit_select = 1, allow_settings_after_format_in_insert = 1"
    fi
    table="${DB}.ddl_parser_replicated_${setting_source}"

    ${CLICKHOUSE_CLIENT} --implicit_select="${session_value}" --allow_settings_after_format_in_insert="${session_value}" \
        --distributed_ddl_output_mode=throw --query "
            CREATE TABLE ${table} ENGINE = Memory AS SELECT
                formatQueryOrNull('1 + 1') AS formatted,
                JSONHas(parseQueryToJSON('INSERT INTO t SELECT 1 FORMAT Null SETTINGS max_threads = 1'), 'settings_ast') AS parsed_settings
            ${settings_clause}" >/dev/null
    ${CLICKHOUSE_CLIENT} --query "SELECT '${setting_source}', 'replicated', formatted, parsed_settings FROM ${table}"
done

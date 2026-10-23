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

${CLICKHOUSE_CLIENT} --query "
    WITH
        [
            'enable_json_ast_dialect',
            'enable_logsql_dialect',
            'logsql_database',
            'logsql_table',
            'logsql_time_column',
            'logsql_message_column',
            'implicit_select',
            'allow_settings_after_format_in_insert'
        ] AS parser_settings,
        ['database ='] AS initiator_only_settings
    SELECT
        count() = 1,
        countIf(NOT arrayExists(setting -> position(value, setting) > 0, parser_settings)) = count(),
        countIf(NOT arrayExists(setting -> position(value, setting) > 0, initiator_only_settings)) = count(),
        countIf(position(value, 'join_use_nulls') > 0) = count()
    FROM system.zookeeper
    WHERE path = '${ZK_PATH}/log'
        AND startsWith(name, 'query-')
        AND position(value, '${TABLE}') > 0
"

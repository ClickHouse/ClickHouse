#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: the DDL worker re-parses the DDL with the session `dialect`, which cannot parse the added comment
# `MODIFY SETTING name = DEFAULT` means a reset for every way a query reaches the server. A JSON AST
# carries the entry in `default_settings` of its `SetQuery` and never passes through the SQL parser.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

JSON_URL="${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_json_default_reset (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS merge_with_ttl_timeout = 10, max_bytes_to_merge_at_max_space_in_pool = 1;
"

echo '-- a change and a reset in one JSON command'
${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary '{"type":"AlterQuery","table":"t_json_default_reset","alter_object":"TABLE","command_list":{"type":"ExpressionList","children":[{"type":"AlterCommand","command_type":"MODIFY_SETTING","settings_changes":{"type":"SetQuery","changes":[{"name":"merge_with_ttl_timeout","value":{"field_type":"UInt64","value":20}}],"default_settings":["max_bytes_to_merge_at_max_space_in_pool"]}}]}}'
$CLICKHOUSE_CLIENT -q "
SELECT extract(create_table_query, 'merge_with_ttl_timeout = (\\d+)'), create_table_query LIKE '%max_bytes_to_merge_at_max_space_in_pool%'
FROM system.tables WHERE database = currentDatabase() AND name = 't_json_default_reset';
"

echo '-- an engine without a reset rejects it on this path too'
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_json_default_memory (x UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 100"
${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary '{"type":"AlterQuery","table":"t_json_default_memory","alter_object":"TABLE","command_list":{"type":"ExpressionList","children":[{"type":"AlterCommand","command_type":"MODIFY_SETTING","settings_changes":{"type":"SetQuery","changes":[],"default_settings":["max_rows_to_keep"]}}]}}' 2>&1 | grep -c -F "NOT_IMPLEMENTED"
$CLICKHOUSE_CLIENT -q "
SELECT extract(create_table_query, 'max_rows_to_keep = (\\d+)')
FROM system.tables WHERE database = currentDatabase() AND name = 't_json_default_memory';
"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_json_default_reset"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_json_default_memory"

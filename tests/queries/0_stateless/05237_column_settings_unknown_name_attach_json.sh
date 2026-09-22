#!/usr/bin/env bash
# The paths to a column `SETTINGS` clause that no `.sql` test can express: a full-definition
# `ATTACH TABLE t UUID '...' (...)`, which states its settings itself and so is checked the way `CREATE`
# is, and the JSON AST dialect, which builds the same `SetQuery` payloads without the SQL parser.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

JSON_URL="${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json"

echo '--- a full-definition ATTACH states its settings, so they are checked ---'
# A literal UUID would collide between parallel runs, since it is server-global.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "
ATTACH TABLE t_column_settings_attach UUID '${uuid}' (x UInt64 SETTINGS (not_a_setting = DEFAULT))
ENGINE = MergeTree ORDER BY x;" 2>&1 >/dev/null | grep -o -m 1 -F 'UNKNOWN_SETTING'
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "
ATTACH TABLE t_column_settings_attach UUID '${uuid}' (x UInt64 SETTINGS (param_not_a_setting = 1))
ENGINE = MergeTree ORDER BY x;" 2>&1 >/dev/null | grep -o -m 1 -F 'UNKNOWN_SETTING'

echo '--- a settable name is still accepted on the same path ---'
# The server warns that a full-definition ATTACH is not recommended, and any stderr fails the test.
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "
ATTACH TABLE t_column_settings_attach UUID '${uuid}' (x UInt64 SETTINGS (min_compress_block_size = 100))
ENGINE = MergeTree ORDER BY x;"
$CLICKHOUSE_CLIENT -q "
SELECT create_table_query LIKE '%SETTINGS (min_compress_block_size = 100)%'
FROM system.tables WHERE database = currentDatabase() AND name = 't_column_settings_attach';"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_column_settings_attach"

echo '--- the JSON AST dialect reaches the same check ---'
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_column_settings_json (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x"

${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary '{"type":"AlterQuery","table":"t_column_settings_json","alter_object":"TABLE","command_list":{"type":"ExpressionList","children":[{"type":"AlterCommand","command_type":"MODIFY_COLUMN","col_decl":{"type":"ColumnDeclaration","name":"y","data_type":{"type":"DataType","name":"UInt64"},"settings":{"type":"SetQuery","default_settings":["not_a_setting"]}}}]}}' 2>&1 | grep -c -F 'UNKNOWN_SETTING'

${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary '{"type":"AlterQuery","table":"t_column_settings_json","alter_object":"TABLE","command_list":{"type":"ExpressionList","children":[{"type":"AlterCommand","command_type":"MODIFY_COLUMN","col_decl":{"type":"ColumnDeclaration","name":"y","data_type":{"type":"DataType","name":"UInt64"},"settings":{"type":"SetQuery","query_parameters":[{"name":"not_a_setting","value":"1"}]}}}]}}' 2>&1 | grep -c -F 'UNKNOWN_SETTING'

echo '--- and a settable name still goes through it ---'
${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary '{"type":"AlterQuery","table":"t_column_settings_json","alter_object":"TABLE","command_list":{"type":"ExpressionList","children":[{"type":"AlterCommand","command_type":"MODIFY_COLUMN","col_decl":{"type":"ColumnDeclaration","name":"y","data_type":{"type":"DataType","name":"UInt64"},"settings":{"type":"SetQuery","changes":[{"name":"min_compress_block_size","value":{"field_type":"UInt64","value":100}}]}}}]}}'
$CLICKHOUSE_CLIENT -q "
SELECT create_table_query LIKE '%\`y\` UInt64 SETTINGS (min_compress_block_size = 100)%'
FROM system.tables WHERE database = currentDatabase() AND name = 't_column_settings_json';"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_column_settings_json"

#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# The HTTP headers of an `HTTP` dictionary source must also be hidden when the query is submitted
# as a JSON AST (`dialect = 'clickhouse_json'`).

CLICKHOUSE_CLIENT_JSON="${CLICKHOUSE_CLIENT} --enable_json_ast_dialect 1 --dialect clickhouse_json"

function to_json()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT parseQueryToJSON(\$\$$1\$\$) FORMAT TSVRaw"
}

# A valid definition submitted as a JSON AST is masked like the SQL one.
JSON=$(to_json "CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.d_05142 (id UInt64, v String) PRIMARY KEY id
    SOURCE(HTTP(url 'http://localhost:11111/x.tsv' format 'TabSeparated' headers(header(name 'API-KEY' value 'SEKRIT_JSON_VALID'))))
    LIFETIME(0) LAYOUT(FLAT())")
${CLICKHOUSE_CLIENT_JSON} --query "$JSON"
${CLICKHOUSE_CLIENT} --query "SELECT extract(create_table_query, 'HEADERS.*\\)\\)\\)') FROM system.tables WHERE database = currentDatabase() AND name = 'd_05142' SETTINGS format_display_secrets_in_show_and_select = 0"

# The SQL parser lower-cases the keys, but a JSON AST can spell them in any case; the secret keys must
# be recognized anyway.
JSON=$(to_json "CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.d_05142_case (id UInt64, v String) PRIMARY KEY id
    SOURCE(HTTP(url 'http://localhost:11111/x.tsv' format 'TabSeparated' credentials(user 'user' password 'SEKRIT_JSON_CASE_PW')
        headers(header(name 'API-KEY' value 'SEKRIT_JSON_CASE'))))
    LIFETIME(0) LAYOUT(FLAT())")
for key in headers header value password
do
    JSON=${JSON//\"first\":\"$key\"/\"first\":\"${key^^}\"}
done
${CLICKHOUSE_CLIENT_JSON} --query "$JSON"
${CLICKHOUSE_CLIENT} --query "SELECT extract(create_table_query, 'CREDENTIALS.*\\)\\)\\)') FROM system.tables WHERE database = currentDatabase() AND name = 'd_05142_case' SETTINGS format_display_secrets_in_show_and_select = 0"

# The accepted JSON query is logged without the value.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0, countIf(query LIKE concat('%', 'SEKRIT', '%'))
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_kind = 'Create' AND type != 'ExceptionBeforeStart' AND query LIKE '%d_05142%' AND event_date >= yesterday()"

${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY d_05142"
${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY d_05142_case"

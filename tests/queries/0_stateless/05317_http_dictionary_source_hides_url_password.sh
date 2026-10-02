#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# The source of an `HTTP` dictionary is shown in `system.dictionaries` without the password of its url.
user="user_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'plain_dictionary_password'"

url="http://${user}:plain_dictionary_password@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number,toString(number)+FROM+numbers(2)+FORMAT+TabSeparated"
$CLICKHOUSE_CLIENT -q "CREATE DICTIONARY dict_http (key UInt64, value String) PRIMARY KEY key SOURCE(HTTP(URL '${url}' FORMAT 'TabSeparated')) LIFETIME(0) LAYOUT(FLAT())"
$CLICKHOUSE_CLIENT -q "SELECT dictGet('dict_http', 'value', 1)"
$CLICKHOUSE_CLIENT -q "SELECT countSubstrings(source, 'plain_dictionary_password'), countSubstrings(source, '[HIDDEN]') FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_http'"

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY dict_http"
$CLICKHOUSE_CLIENT -q "DROP USER ${user}"

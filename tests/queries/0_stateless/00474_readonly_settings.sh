#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="select toUInt64(pow(2, 62)) as value format JSON" --output_format_json_quote_64bit_integers=0 | grep value
$CLICKHOUSE_CLIENT --query="select toUInt64(pow(2, 62)) as value format JSON" --output_format_json_quote_64bit_integers=1 | grep value

$CLICKHOUSE_CLIENT --readonly=1 --multiquery --query="set output_format_json_quote_64bit_integers=1 ; select toUInt64(pow(2, 63)) as value format JSON" --server_logs_file=/dev/null 2>&1 | grep -o 'value\|Cannot modify .* setting in readonly mode'
$CLICKHOUSE_CLIENT --readonly=1 --multiquery --query="set output_format_json_quote_64bit_integers=0 ; select toUInt64(pow(2, 63)) as value format JSON" --server_logs_file=/dev/null 2>&1 | grep -o 'value\|Cannot modify .* setting in readonly mode'

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=1" | grep value
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=0" | grep value

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=readonly&session_timeout=3600" -d 'SET readonly = 1'

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=readonly&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=1" 2>&1 | grep -o 'value\|Cannot modify .* setting in readonly mode.'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=readonly&query=SELECT+toUInt64(pow(2,+63))+as+value+format+JSON&output_format_json_quote_64bit_integers=0" 2>&1 | grep -o 'value\|Cannot modify .* setting in readonly mode'

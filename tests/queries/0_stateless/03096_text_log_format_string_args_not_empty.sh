#!/bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id=$(random_str 12)
$CLICKHOUSE_CLIENT -m --enable_analyzer=1 --query_id ${query_id}_1 --query "select count; -- { serverError UNKNOWN_IDENTIFIER }"
$CLICKHOUSE_CLIENT -m --enable_analyzer=1 --query_id ${query_id}_2 --query "select conut(); -- { serverError UNKNOWN_FUNCTION }"

$CLICKHOUSE_CLIENT -m -q "
system flush logs text_log;

SET max_rows_to_read = 0; -- system.text_log can be really big

select count() > 0 from system.text_log where event_date >= yesterday() and level = 'Error' and message_format_string = 'Unknown {}{} identifier {} in scope {}{}' and value1 = 'expression' and value3 = '\`count\`' and value4 = 'SELECT count' and query_id = '${query_id}_1';

select count() > 0 from system.text_log where event_date >= yesterday() and level = 'Error' and message_format_string = 'Function with name {} does not exist. In scope {}{}' and value1 = '\`conut\`' and value2 = 'SELECT conut()' and value3 != '' and query_id = '${query_id}_2';
"

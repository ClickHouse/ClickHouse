#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment, because the test has to set its own
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

file1="$CUR_DIR/clickhouse.${CLICKHOUSE_DATABASE}-1.sql"
echo -n 'select 42' >> "$file1"
file2="$CUR_DIR/clickhouse.${CLICKHOUSE_DATABASE}-2.sql"
echo -n 'select 4242' >> "$file2"

$CLICKHOUSE_CLIENT --queries-file "$file1" "$file2" <<<'select 42'
$CLICKHOUSE_CLIENT --log_comment foo --queries-file /dev/stdin <<<'select 424242'

$CLICKHOUSE_CLIENT -m -q "
    system flush logs;
    select query, log_comment from system.query_log where current_database = '$CLICKHOUSE_DATABASE' and event_date >= yesterday() and query = 'select 42' and type != 'QueryStart';
    select query, log_comment from system.query_log where current_database = '$CLICKHOUSE_DATABASE' and event_date >= yesterday() and query = 'select 4242' and type != 'QueryStart';
    select query, log_comment from system.query_log where current_database = '$CLICKHOUSE_DATABASE' and event_date >= yesterday() and query = 'select 424242\n' and type != 'QueryStart';
" | sed "s#$CUR_DIR/##"

rm "$file1" "$file2"

#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --queries-file /dev/stdin <<<'select 42'
$CLICKHOUSE_CLIENT -nm -q "
    system flush logs;
    select query, log_comment from system.query_log where current_database = '$CLICKHOUSE_DATABASE' and event_date >= yesterday() and query = 'select 42\n' and type != 'QueryStart';
"

$CLICKHOUSE_CLIENT --log_comment foo --queries-file /dev/stdin <<<'select 4242'
$CLICKHOUSE_CLIENT -nm -q "
    system flush logs;
    select query, log_comment from system.query_log where current_database = '$CLICKHOUSE_DATABASE' and event_date >= yesterday() and query = 'select 4242\n' and type != 'QueryStart';
"

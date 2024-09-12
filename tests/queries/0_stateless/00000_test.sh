#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query_id="my_id_$CLICKHOUSE_TEST_UNIQUE_NAME" -q "select 42";
$CLICKHOUSE_CLIENT -q "system flush logs";
$CLICKHOUSE_CLIENT -q "select query from system.query_log where query_id='my_id_$CLICKHOUSE_TEST_UNIQUE_NAME' and type='QueryFinish'"


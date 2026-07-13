#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "drop table if exists test"
$CLICKHOUSE_CLIENT -q "create table test (x UInt32) engine=Memory";

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1"

$CLICKHOUSE_LOCAL -q "select NULL::Nullable(UInt32) as x format Native" | ${CLICKHOUSE_CURL} -sS "$url&query=INSERT%20INTO%20test%20FORMAT%20Native" --data-binary @-

$CLICKHOUSE_CLIENT -q "select * from test";
$CLICKHOUSE_CLIENT -q "drop table test"


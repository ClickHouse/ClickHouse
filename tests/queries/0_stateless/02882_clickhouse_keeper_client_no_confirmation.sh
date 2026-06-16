#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

path="/test-keeper-client-$CLICKHOUSE_DATABASE"

$CLICKHOUSE_KEEPER_CLIENT -q "rm '$path'" >& /dev/null

$CLICKHOUSE_KEEPER_CLIENT -q "create '$path' 'foobar'"
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'"
$CLICKHOUSE_KEEPER_CLIENT -q "get '$path'" 2>&1

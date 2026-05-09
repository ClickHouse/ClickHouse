#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

path="/test-keeper-client-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'" >& /dev/null || true

$CLICKHOUSE_KEEPER_CLIENT -q "create '$path' 'root'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/T' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/T/A' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/T/A/B' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/T/C' 'x'"

echo 'lsr explicit path'
$CLICKHOUSE_KEEPER_CLIENT -q "lsr '$path' 100"

echo 'lsr path 2'
$CLICKHOUSE_KEEPER_CLIENT -q "lsr '$path' 2"

echo 'lsr from cwd'
$CLICKHOUSE_KEEPER_CLIENT -q "cd '$path'; lsr 100"

echo 'cleanup'
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'"

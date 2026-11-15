#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

path="/test-keeper-client-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_KEEPER_CLIENT -q "rm '$path'" >& /dev/null

$CLICKHOUSE_KEEPER_CLIENT -q "create '$path' 'root'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/A' 'node-A'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/A/B' 'node-B'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/C' 'node-B'"

echo 'initial'
$CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'"

echo 'simple copy'
$CLICKHOUSE_KEEPER_CLIENT -q "cp '$path/A' '$path/D'"
$CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'"
$CLICKHOUSE_KEEPER_CLIENT -q "get '$path/D'"

echo 'simple move'
$CLICKHOUSE_KEEPER_CLIENT -q "mv '$path/D' '$path/H'"
$CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'"
$CLICKHOUSE_KEEPER_CLIENT -q "get '$path/H'"

echo 'move node with childs -- must be error'
$CLICKHOUSE_KEEPER_CLIENT -q "mv '$path/A' '$path/ERROR'" 2>&1
$CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'"

echo 'move node to existing'
$CLICKHOUSE_KEEPER_CLIENT -q "mv '$path/C' '$path/A'" 2>&1
$CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'"

echo 'clean up'
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'"

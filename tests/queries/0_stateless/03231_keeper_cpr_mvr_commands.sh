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

echo 'copy'
$CLICKHOUSE_KEEPER_CLIENT -q "cpr '$path/A' '$path/D'"
$CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'"
$CLICKHOUSE_KEEPER_CLIENT -q "get '$path/D'"
$CLICKHOUSE_KEEPER_CLIENT -q "get '$path/D/B'"

echo 'move'
$CLICKHOUSE_KEEPER_CLIENT -q "mvr '$path/D' '$path/H'"
$CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'"
$CLICKHOUSE_KEEPER_CLIENT -q "get '$path/H'"
$CLICKHOUSE_KEEPER_CLIENT -q "get '$path/H/B'"

echo 'copy to existing'
$CLICKHOUSE_KEEPER_CLIENT -q "cpr '$path/H' '$path/C'" 2>&1
$CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'"

echo 'move to existing'
$CLICKHOUSE_KEEPER_CLIENT -q "mvr '$path/H' '$path/C'" 2>&1
$CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'"

echo 'clean up'
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'"

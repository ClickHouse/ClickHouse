#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

path="/test-keeper-client-$CLICKHOUSE_DATABASE"

$CLICKHOUSE_KEEPER_CLIENT -q "rm '$path'" >& /dev/null

$CLICKHOUSE_KEEPER_CLIENT -q "create '$path' 'foobar'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/1' 'foobar'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/1/a' 'foobar'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/1/a/a' 'foobar'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/1/b' 'foobar'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/1/c' 'foobar'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/1/d' 'foobar'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/1/d/a' 'foobar'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/1/d/b' 'foobar'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/1/d/c' 'foobar'"

echo 'find_super_nodes'
$CLICKHOUSE_KEEPER_CLIENT -q "find_super_nodes 1000000000"
$CLICKHOUSE_KEEPER_CLIENT -q "find_super_nodes 3 '$path'" | sort

echo 'find_big_family'
$CLICKHOUSE_KEEPER_CLIENT -q "find_big_family '$path' 3"

$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'"

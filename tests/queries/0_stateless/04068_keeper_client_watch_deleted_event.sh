#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

path="/test-keeper-watch-del-$CLICKHOUSE_DATABASE"

$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'" >& /dev/null

$CLICKHOUSE_KEEPER_CLIENT -q "create '$path' 'initial_value'"

# -- DELETED event via get watch --
echo '-- deleted event'
$CLICKHOUSE_KEEPER_CLIENT -q "get '$path' wdel; rm '$path'; watch wdel 10" 2>&1 | sed "s|$path|/test_path|g"

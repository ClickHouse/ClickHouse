#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

path="/test-keeper-watch-$CLICKHOUSE_DATABASE"

$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'" >& /dev/null

$CLICKHOUSE_KEEPER_CLIENT -q "create '$path' 'initial'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/child1' 'c1'"

# -- get with watch: compatible output (prints value as before) --
echo '-- get with watch_id'
$CLICKHOUSE_KEEPER_CLIENT -q "get '$path' w1"

# -- exists with watch: compatible output (prints 1/0 as before) --
echo '-- exists with watch_id'
$CLICKHOUSE_KEEPER_CLIENT -q "exists '$path' w2"
$CLICKHOUSE_KEEPER_CLIENT -q "exists '$path/nonexistent' w3"

# -- ls with watch: compatible output (prints children as before) --
echo '-- ls with watch_id'
$CLICKHOUSE_KEEPER_CLIENT -q "ls '$path' w4"

# -- watch with data change (CHANGED event via get watch) --
echo '-- watch data change'
$CLICKHOUSE_KEEPER_CLIENT -q "get '$path' wdata; set '$path' 'updated'; watch wdata 10"

# -- watch child event (CHILD event via ls watch) --
echo '-- watch child event'
$CLICKHOUSE_KEEPER_CLIENT -q "ls '$path' wchild; create '$path/child2' 'c2'; watch wchild 10"

# -- watch exists on non-existent node (CREATED event) --
echo '-- watch exists created'
$CLICKHOUSE_KEEPER_CLIENT -q "exists '$path/will_appear' wexist; create '$path/will_appear' 'hi'; watch wexist 10"

# -- watch timeout --
echo '-- watch timeout'
$CLICKHOUSE_KEEPER_CLIENT -q "get '$path' wtimeout; watch wtimeout 0.5" 2>&1

# -- watch with unknown id --
echo '-- watch unknown id'
$CLICKHOUSE_KEEPER_CLIENT -q "watch no_such_watch 1" 2>&1

# -- duplicate watches --
echo '-- duplicate watches' 2>&1
$CLICKHOUSE_KEEPER_CLIENT -q "ls '$path' wexists_ls; get '$path' wexists_ls" 2>&1

# Cleanup
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'"

#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Create test znodes for path completion tests
path="/test-keeper-autocomplete-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'" >& /dev/null

$CLICKHOUSE_KEEPER_CLIENT -q "create '$path' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/dir_node' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/dir_node/child1' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/dir_node/child2' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/leaf_node' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/unique_prefix_xyz' 'x'"

python3 "$CUR_DIR"/03988_keeper_client_autocomplete.python "$path"

$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'" >& /dev/null

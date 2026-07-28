#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Query parameters passed via --param_<name> on the command line must not be corrupted
# when <name> collides with a builtin setting name (issue #91168).
$CLICKHOUSE_CLIENT --param_max_threads="foobar" --query "SELECT {max_threads:String}"
$CLICKHOUSE_CLIENT --param_limit=2 --query "SELECT number FROM numbers(10) LIMIT {limit:UInt8}"
$CLICKHOUSE_CLIENT --param_offset=3 --query "SELECT {offset:UInt32}"


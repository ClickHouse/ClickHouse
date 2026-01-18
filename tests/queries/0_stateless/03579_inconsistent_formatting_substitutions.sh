#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_FORMAT --query 'SELECT {$abc:String}' | $CLICKHOUSE_FORMAT

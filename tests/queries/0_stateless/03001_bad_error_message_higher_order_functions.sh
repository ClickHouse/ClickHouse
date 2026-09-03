#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "SELECT arrayMap((x,y) -> x + y, [1,2,3], [1,2])" 2>&1 | grep -o -F --max-count 1 'Argument 3 has size 2 which differs with the size of another argument, 3'
#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --external --file <(echo "Hello, world") --name test --format CSV --structure "x Enum('Hello' = 1, 'world' = 2), y String" --query "SELECT * FROM test"

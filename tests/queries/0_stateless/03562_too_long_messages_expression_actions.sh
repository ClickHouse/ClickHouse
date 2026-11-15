#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL "SELECT arraySum(arrayMap(x -> arraySum(arrayMap(y -> arraySum(arrayMap(z -> sin(x + y + z) * cos(x * y * z) * tanh(x - y + z), range(100))), range(1000))), range(10000))) SETTINGS max_memory_usage = '1G'" 2>&1 |
    $CLICKHOUSE_LOCAL --input-format LineAsString -q "length(line) < 1000"

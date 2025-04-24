#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "SELECT count(*) FROM numbers(10) AS a, numbers(11) AS b, numbers(12) AS c SETTINGS max_block_size = 0" 2>&1 | grep -q "Sanity check: 'max_block_size' cannot be 0. Set to default value" && echo "OK" || echo "FAIL"

#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query with two seconds sleep cannot be processed with QPS > 0.5
$CLICKHOUSE_BENCHMARK --query "SELECT sleep(2)" 2>&1 | grep -m1 -o -P 'QPS: \d+\.\d+' | $CLICKHOUSE_LOCAL --query "SELECT throwIf(extract(line, 'QPS: (.+)')::Float64 > 0.75) FROM table" --input-format LineAsString

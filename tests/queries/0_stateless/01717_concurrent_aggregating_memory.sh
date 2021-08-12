#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "CREATE TABLE aggr ENGINE = AggregatingMemory AS SELECT groupArray(number) FROM numbers(1)"
$CLICKHOUSE_BENCHMARK <<< "INSERT INTO aggr SELECT * FROM numbers(6)" -i 100 -c 100 2>/dev/null
$CLICKHOUSE_CLIENT -q "SELECT * FROM aggr"

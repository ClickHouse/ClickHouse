#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query="SELECT 1 format CSVt" 2>&1 | grep -q "Maybe you meant: \['CSV','TSV'\]" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_LOCAL --query="SELECT 1 format PrettyJSON" 2>&1 | grep -q "Maybe you meant: \['PrettyNDJSON'\]" && echo 'OK' || echo 'FAIL'

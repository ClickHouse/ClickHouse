#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select repeat('[', 10000) || '1,2,3' || repeat(']', 10000)" > 02501_deep_nested_array.tsv
$CLICKHOUSE_LOCAL -q "desc file(02501_deep_nested_array.tsv)" 2>&1 | grep -q -F "TOO_DEEP_RECURSION" && echo "OK" || echo "FAIL"
rm 02501_deep_nested_array.tsv


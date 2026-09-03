#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -sS --data-binary "SELECT rand64(1) % 10 AS k, groupArray(toString(64 - floor(log2(rand64(2) + 1))))::Array(LowCardinality(String)) FROM numbers(100) GROUP BY k FORMAT RawBLOB" | grep -o -F 'NOT_IMPLEMENTED'

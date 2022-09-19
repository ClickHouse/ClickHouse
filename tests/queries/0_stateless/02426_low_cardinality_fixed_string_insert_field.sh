#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --structure 'x LowCardinality(FixedString(2454139))' --input-format Values --output-format TSV --query "SELECT * FROM table" <<< '(1)' | wc -c

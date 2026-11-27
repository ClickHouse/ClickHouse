#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select toLowCardinality(toString(number % 10)) as x from numbers(20) format Arrow settings max_block_size=7, output_format_arrow_low_cardinality_as_dictionary=1" | $CLICKHOUSE_LOCAL -q "select * from table order by x" --input-format='Arrow'


#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "insert into table function file('arrow.dict', 'Arrow', 'x LowCardinality(UInt64)') select number from numbers(10) settings output_format_arrow_low_cardinality_as_dictionary=1, engine_file_truncate_on_insert=1"

$CLICKHOUSE_CLIENT -q "desc file('arrow.dict', 'Arrow')"


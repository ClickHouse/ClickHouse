#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select true::LowCardinality(Bool) as a format Arrow settings allow_suspicious_low_cardinality_types = 1, output_format_arrow_low_cardinality_as_dictionary = 1" | $CLICKHOUSE_LOCAL -q "select * from table" --input-format=Arrow
$CLICKHOUSE_LOCAL -q "select '2020-01-01'::LowCardinality(Date32) as a format Arrow settings allow_suspicious_low_cardinality_types = 1, output_format_arrow_low_cardinality_as_dictionary = 1" | $CLICKHOUSE_LOCAL -q "select * from table" --input-format=Arrow

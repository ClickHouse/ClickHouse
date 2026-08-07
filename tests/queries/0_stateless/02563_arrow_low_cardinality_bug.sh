#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test"
$CLICKHOUSE_CLIENT -q "create table test (z LowCardinality(Nullable(String))) engine=Memory";
$CLICKHOUSE_CLIENT -q "select CAST(number % 2 ? NULL : toString(number), 'LowCardinality(Nullable(String))') as z from numbers(2) format Arrow settings output_format_arrow_low_cardinality_as_dictionary=1" |  $CLICKHOUSE_CLIENT -q "insert into test format Arrow"

$CLICKHOUSE_CLIENT -q "select * from test"
$CLICKHOUSE_CLIENT -q "drop table test"


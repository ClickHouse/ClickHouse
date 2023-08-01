#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select toInt32(-10000)::Date32 as d format Avro" | $CLICKHOUSE_LOCAL --input-format=Avro -q "select toTypeName(d), d from table"

$CLICKHOUSE_LOCAL -q "select 4242.4242::Decimal32(4) as d format Avro" | $CLICKHOUSE_LOCAL --input-format=Avro -q "select toTypeName(d), d from table"
$CLICKHOUSE_LOCAL -q "select 4242.4242::Decimal64(14) as d format Avro" | $CLICKHOUSE_LOCAL --input-format=Avro -q "select toTypeName(d), d from table"
$CLICKHOUSE_LOCAL -q "select 4242.4242::Decimal128(34) as d format Avro" | $CLICKHOUSE_LOCAL --input-format=Avro -q "select toTypeName(d), d from table"
$CLICKHOUSE_LOCAL -q "select 4242.4242::Decimal256(64) as d format Avro" | $CLICKHOUSE_LOCAL --input-format=Avro -q "select toTypeName(d), d from table"


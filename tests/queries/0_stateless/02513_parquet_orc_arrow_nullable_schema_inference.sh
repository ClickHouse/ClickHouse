#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select * from numbers(3) format Parquet" | $CLICKHOUSE_LOCAL --input-format=Parquet --table=test -q "desc test" --schema_inference_make_columns_nullable=1;
$CLICKHOUSE_LOCAL -q "select * from numbers(3) format Parquet" | $CLICKHOUSE_LOCAL --input-format=Parquet --table=test -q "desc test" --schema_inference_make_columns_nullable=0;

$CLICKHOUSE_LOCAL -q "select * from numbers(3) format ORC" | $CLICKHOUSE_LOCAL --input-format=ORC --table=test -q "desc test" --schema_inference_make_columns_nullable=1;
$CLICKHOUSE_LOCAL -q "select * from numbers(3) format ORC" | $CLICKHOUSE_LOCAL --input-format=ORC --table=test -q "desc test" --schema_inference_make_columns_nullable=0;

$CLICKHOUSE_LOCAL -q "select * from numbers(3) format Arrow" | $CLICKHOUSE_LOCAL --input-format=Arrow --table=test -q "desc test" --schema_inference_make_columns_nullable=1;
$CLICKHOUSE_LOCAL -q "select * from numbers(3) format Arrow" | $CLICKHOUSE_LOCAL --input-format=Arrow --table=test -q "desc test" --schema_inference_make_columns_nullable=0;


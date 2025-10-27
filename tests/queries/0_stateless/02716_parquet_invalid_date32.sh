#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/02716_data.parquet', auto, 'date Date32')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/02716_data.parquet', auto, 'date Int32')"


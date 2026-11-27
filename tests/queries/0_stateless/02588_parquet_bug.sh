#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/02588_data.parquet') where exchange_ts = 1670964058771367936"
$CLICKHOUSE_LOCAL -q "select exchange_ts from file('$CURDIR/data_parquet/02588_data.parquet') where exchange_ts = 1670964058771367936"
$CLICKHOUSE_LOCAL -q "select exchange_ts, market, product from file('$CURDIR/data_parquet/02588_data.parquet') where exchange_ts = 1670946478544048640"


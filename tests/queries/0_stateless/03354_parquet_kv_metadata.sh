#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_parquet/03354.parquet', ParquetKeyValueMetadata) format JSONEachRow"

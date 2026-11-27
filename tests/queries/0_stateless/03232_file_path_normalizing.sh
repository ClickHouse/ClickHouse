#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "SELECT substring(_path, position(_path, 'data_hive')) FROM file('$CURDIR/data_hive/partitioning/non_existing_column=*/sample.parquet') LIMIT 1;"

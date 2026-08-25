#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/87322
# Accessing Tuple element subcolumns with delta.columnMapping.mode = 'name'.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
SELECT c1.values FROM deltaLakeLocal('$CUR_DIR/data_delta_lake/struct_column_mapping') ORDER BY c1.values;
SELECT c1.id FROM deltaLakeLocal('$CUR_DIR/data_delta_lake/struct_column_mapping') ORDER BY c1.id;
SELECT c1 FROM deltaLakeLocal('$CUR_DIR/data_delta_lake/struct_column_mapping') ORDER BY c1.values;
"

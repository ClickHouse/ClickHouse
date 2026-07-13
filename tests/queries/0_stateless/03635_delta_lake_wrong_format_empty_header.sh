#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


$CLICKHOUSE_LOCAL -q "
SELECT t1d0.c1 FROM deltaLakeLocal('$CUR_DIR/data_delta_lake/lakehouses/spark_catalog/test/t0', 'CustomSeparated', 'c1 Int256, c0 DateTime64, c2 Dynamic') AS t0d0 RIGHT JOIN (select 42 as c1, 42 as c2) AS t1d0 ON t0d0._data_lake_snapshot_version = t1d0.c2; --{serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED}
"

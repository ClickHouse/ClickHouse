#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query_id="test_03231_1" --query "
    SELECT countDistinct(_path) FROM file('$CURDIR/data_hive/partitioning/column0=*/sample.parquet') WHERE column0 = 'Elizabeth';
"

${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['EngineFileLikeReadFiles'] FROM system.query_log WHERE query_id='test_03231_1';
"

${CLICKHOUSE_CLIENT} --query_id="test_03231_2" --query "
    SELECT countDistinct(_path) FROM file('$CURDIR/data_hive/partitioning/identifier=*/email.csv') WHERE identifier = 2070;
"

${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['EngineFileLikeReadFiles'] FROM system.query_log WHERE query_id='test_03231_2';
"

${CLICKHOUSE_CLIENT} --query_id="test_03231_3" --query "
    SELECT countDistinct(_path) FROM file('$CURDIR/data_hive/partitioning/array=*/sample.parquet') WHERE array = [1,2,3];
"

${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['EngineFileLikeReadFiles'] FROM system.query_log WHERE query_id='test_03231_3';
"

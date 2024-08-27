#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query_id="test_03231_1" --query "
    SET use_hive_partitioning = 1;
"

$CLICKHOUSE_LOCAL --query_id="test_03231_1" --query "
    SELECT countDistinct(_path) FROM file('$CURDIR/data_hive/partitioning/column0=*/sample.parquet') WHERE column0 = 'Elizabeth';
"

$CLICKHOUSE_LOCAL --query "
    SELECT ProfileEvents['EngineFileLikeReadFiles'] FROM system.query_log WHERE query_id='test_03231_1' WHERE current_database = currentDatabase();
"

$CLICKHOUSE_LOCAL --query_id="test_03231_2" --query "
    SELECT countDistinct(_path) FROM file('$CURDIR/data_hive/partitioning/identifier=*/email.csv') WHERE identifier = 2070;
"

$CLICKHOUSE_LOCAL --query "
    SELECT ProfileEvents['EngineFileLikeReadFiles'] FROM system.query_log WHERE query_id='test_03231_2' WHERE current_database = currentDatabase();
"

$CLICKHOUSE_LOCAL --query_id="test_03231_3" --query "
    SELECT countDistinct(_path) FROM file('$CURDIR/data_hive/partitioning/array=*/sample.parquet') WHERE array = [1,2,3];
"

$CLICKHOUSE_LOCAL --query "
    SELECT ProfileEvents['EngineFileLikeReadFiles'] FROM system.query_log WHERE query_id='test_03231_3' WHERE current_database = currentDatabase();
"

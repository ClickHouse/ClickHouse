#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -m -q "CREATE TABLE test_table (geom MultiPolygon) engine=MergeTree ORDER BY geom;
INSERT INTO test_table SELECT * FROM file('$CURDIR/data_parquet/02960_polygon_bound_bug.parquet', Parquet);
CREATE DICTIONARY test_dict (geom MultiPolygon) PRIMARY KEY geom SOURCE (CLICKHOUSE(TABLE 'test_table')) LIFETIME(MIN 0 MAX 0) LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1));
SELECT dictHas(test_dict,(174.84729269276494,-36.99524960275426));"

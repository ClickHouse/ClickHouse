#!/usr/bin/env bash
# Tags: no-fasttest
# Test for https://github.com/ClickHouse/ClickHouse/issues/107380
# Reading hive partitioned files must work when WHERE filters a real (non-virtual)
# column while a hive partition column is also selected.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR="demo_$CLICKHOUSE_TEST_UNIQUE_NAME"

$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION file('$DIR/date=2026-01-01/data.parquet', 'Parquet', 'name String, age Int64')
SETTINGS engine_file_truncate_on_insert = 1
SELECT * FROM values('name String, age Int64', ('name1', 20), ('name2', 21), ('name3', 22));

INSERT INTO FUNCTION file('$DIR/date=2026-02-02/data.parquet', 'Parquet', 'name String, age Int64')
SETTINGS engine_file_truncate_on_insert = 1
SELECT * FROM values('name String, age Int64', ('name4', 30), ('name5', 31));
"

$CLICKHOUSE_CLIENT --use_hive_partitioning=1 -q "
-- WHERE on hive virtual column (was already fine)
SELECT date, age FROM file('$DIR/date=*/data.parquet') WHERE date > '2026-01-15' ORDER BY age;

-- WHERE on real parquet column while selecting a hive column (the bug)
SELECT date, age FROM file('$DIR/date=*/data.parquet') WHERE age > 20 ORDER BY age;

-- WHERE on both a real and a hive column
SELECT date, age FROM file('$DIR/date=*/data.parquet') WHERE age > 20 AND date > '2026-01-15' ORDER BY age;

-- PREWHERE on real column while selecting a hive column
SELECT date, age FROM file('$DIR/date=*/data.parquet') PREWHERE age > 20 ORDER BY age;

-- Select only the hive column, filter on a real column
SELECT date FROM file('$DIR/date=*/data.parquet') WHERE age > 21 ORDER BY date;
"

DATA_FILE_PATH=$($CLICKHOUSE_CLIENT -q "SELECT _path FROM file('$DIR/date=2026-01-01/data.parquet') LIMIT 1")
rm -rf "$(dirname "$(dirname "$DATA_FILE_PATH")")"

#!/usr/bin/env bash
# Tags: no-fasttest

# repro for https://github.com/ClickHouse/ClickHouse/issues/98801
# hive partitioning auto-inference fails to parse numeric values with leading zeros

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_DIR=$USER_FILES_PATH/$CLICKHOUSE_TEST_UNIQUE_NAME

mkdir -p "$DATA_DIR/hour=11"
echo -e "id\n1" > "$DATA_DIR/hour=11/data.csv"

mkdir -p "$DATA_DIR/hour=01"
echo -e "id\n2" > "$DATA_DIR/hour=01/data.csv"

$CLICKHOUSE_CLIENT --query "
    SELECT id, hour
    FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/hour=*/data.csv')
    ORDER BY id
    SETTINGS use_hive_partitioning=1;
"

rm -rf "$DATA_DIR"

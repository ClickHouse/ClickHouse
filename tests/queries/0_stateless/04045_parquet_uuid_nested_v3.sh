#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.parquet

$CLICKHOUSE_LOCAL -q "
    CREATE TABLE export_table (opt_id Nullable(UUID), arr_id Array(UUID)) ENGINE = Memory;
    INSERT INTO export_table VALUES (NULL, ['b7f4341e-2cbc-489a-acd6-fae97bdc54a1', '4867f717-c20e-4efc-b04b-6bf04793473f']);
    INSERT INTO export_table VALUES ('6c1799c6-1ceb-45d3-a697-61956cd5a47a', []);
    SELECT * FROM export_table INTO OUTFILE '$DATA_FILE' FORMAT Parquet;
"

# Read without schema to ensure nested inference works
$CLICKHOUSE_LOCAL -q "
    SELECT toTypeName(opt_id), opt_id, toTypeName(arr_id), arr_id 
    FROM file('$DATA_FILE', 'Parquet') ORDER BY opt_id ASC;
"

rm -f $DATA_FILE
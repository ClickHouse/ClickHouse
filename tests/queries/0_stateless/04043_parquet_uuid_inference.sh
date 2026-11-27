#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.parquet

$CLICKHOUSE_LOCAL -q "
    CREATE TABLE export_table (id UUID) ENGINE = Memory;
    INSERT INTO export_table VALUES ('57033795-afc7-42d2-ae07-3943d0395dc4');
    SELECT * FROM export_table INTO OUTFILE '$DATA_FILE' FORMAT Parquet;
"

# Must infer the schema strictly from the Parquet metadata footer
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(id), id FROM file('$DATA_FILE', 'Parquet');"

rm -f $DATA_FILE
#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.arrow

$CLICKHOUSE_LOCAL -q "
    CREATE TABLE export_table (opt_id Nullable(UUID), arr_id Array(UUID)) ENGINE = Memory;
    INSERT INTO export_table VALUES (NULL, ['4b84c7c7-20da-4984-8208-b7f0d49897d8', '56063faf-2bcc-4c9f-a273-c18d25bfc7a3']);
    INSERT INTO export_table VALUES ('f803bc34-2b43-41d4-bc3d-989bb3b369e2', []);
    SELECT * FROM export_table INTO OUTFILE '$DATA_FILE' FORMAT Arrow;
"

# Read with explicit schema hint to bypass Arrow's nested inference limitations
$CLICKHOUSE_LOCAL -q "
    SELECT toTypeName(opt_id), opt_id, toTypeName(arr_id), arr_id 
    FROM file('$DATA_FILE', 'Arrow', 'opt_id Nullable(UUID), arr_id Array(UUID)') ORDER BY opt_id ASC;
"

rm -f $DATA_FILE
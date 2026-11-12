#!/usr/bin/env bash

# The creation of an Ordinary database emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ORD="${CLICKHOUSE_DATABASE}_ord"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${ORD}"
$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 -q "CREATE DATABASE ${ORD} ENGINE = Ordinary"

# Emulates a table stored before the check existed in a clickhouse-local data directory: a well-typed
# index is created, its stored definition is rewritten to the ill-typed one, and the next start loads it.
data_dir="${CLICKHOUSE_TMP}/data"
local_query()
{
    $CLICKHOUSE_LOCAL --path="$data_dir" "$@"
}
attach_stored_ill_typed()
{
    local_query -q "CREATE TABLE $1 (c0 String, c1 Int8, INDEX i0 c0 = toString(c1) TYPE set(0)) ENGINE = MergeTree ORDER BY c1"
    $CLICKHOUSE_LOCAL --param_metadata="$data_dir/metadata/default/$1.sql" --param_copy="$data_dir.sql" -q "
        SET engine_file_truncate_on_insert = 1;
        INSERT INTO FUNCTION file({copy:String}, 'RawBLOB')
            SELECT replaceOne(raw_blob, 'c0 = toString(c1)', 'c0 = c1') FROM file({metadata:String}, 'RawBLOB');
        INSERT INTO FUNCTION file({metadata:String}, 'RawBLOB') SELECT * FROM file({copy:String}, 'RawBLOB')"
    local_query -q "SELECT 1 FROM $1 LIMIT 0"
}

echo 'D0 a full-definition ATTACH of such an index is rejected like CREATE'
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${ORD}.g (c0 String, c1 Int8, INDEX i0 c0 = c1 TYPE set(0)) ENGINE = MergeTree ORDER BY c1" 2>&1 | grep -c 'NO_COMMON_TYPE'

# A table declared before this check existed keeps loading, so an upgrade cannot strand one.
echo 'D1 a table already carrying such an index still attaches'
attach_stored_ill_typed g && echo ok

echo 'D2 an unrelated ALTER on it is still allowed'
local_query -q "ALTER TABLE g ADD COLUMN x UInt8" && echo ok

echo 'D3 a new index the ALTER declares itself is still rejected'
local_query -q "ALTER TABLE g ADD INDEX i1 c1 = c0 TYPE set(0)" 2>&1 | grep -c 'NO_COMMON_TYPE'

echo 'D4 a well-typed new index on the same table is accepted'
local_query -q "ALTER TABLE g ADD INDEX i2 c1 + 1 TYPE minmax" && echo ok
local_query -q "SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'g' ORDER BY name"

echo 'D5 dropping it repairs the table'
local_query -q "ALTER TABLE g DROP INDEX i0" && echo ok
# An inline VALUES list still leaves the client reading stdin, which never closes here.
local_query -q "INSERT INTO g VALUES ('a', 1, 0)" < /dev/null
local_query -q "INSERT INTO g VALUES ('b', 2, 0)" < /dev/null
local_query -q "OPTIMIZE TABLE g FINAL" && echo 'merge ok'
local_query -q "SELECT count() FROM g"

echo 'D6 an already-unevaluable index of the same name is not re-checked'
attach_stored_ill_typed g2
local_query -q "ALTER TABLE g2 DROP INDEX i0, ADD INDEX i0 c0 = c1 TYPE minmax" && echo ok
local_query -q "SELECT type FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'g2' AND name = 'i0'"
local_query -q "ALTER TABLE g2 ADD COLUMN x UInt8" && echo ok

echo 'D7 renaming a column the index references is still allowed'
attach_stored_ill_typed g3
local_query -q "ALTER TABLE g3 RENAME COLUMN c0 TO c2" && echo ok
local_query -q "SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'g3' AND name = 'i0'"
local_query -q "ALTER TABLE g3 DROP INDEX i0" && echo ok

rm -rf "$data_dir" "$data_dir.sql"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${ORD} SYNC"

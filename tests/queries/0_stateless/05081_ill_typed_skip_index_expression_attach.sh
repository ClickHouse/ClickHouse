#!/usr/bin/env bash

# ATTACH TABLE with an inline definition is only accepted by the Ordinary database engine,
# whose creation emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ORD="${CLICKHOUSE_DATABASE}_ord"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${ORD}"
$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 -q "CREATE DATABASE ${ORD} ENGINE = Ordinary"

# A table declared before this check existed keeps loading, so an upgrade cannot strand one.
echo 'D1 a table already carrying such an index still attaches'
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${ORD}.g (c0 String, c1 Int8, INDEX i0 c0 = c1 TYPE set(0)) ENGINE = MergeTree ORDER BY c1" \
    && echo ok

echo 'D2 an unrelated ALTER on it is still allowed'
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.g ADD COLUMN x UInt8" && echo ok

echo 'D3 a new index the ALTER declares itself is still rejected'
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.g ADD INDEX i1 c1 = c0 TYPE set(0)" 2>&1 | grep -c 'NO_COMMON_TYPE'

echo 'D4 a well-typed new index on the same table is accepted'
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.g ADD INDEX i2 c1 + 1 TYPE minmax" && echo ok
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.data_skipping_indices WHERE database = '${ORD}' AND table = 'g' ORDER BY name"

echo 'D5 dropping it repairs the table'
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.g DROP INDEX i0" && echo ok
# An inline VALUES list still leaves the client reading stdin, which never closes here.
$CLICKHOUSE_CLIENT -q "INSERT INTO ${ORD}.g VALUES ('a', 1, 0)" < /dev/null
$CLICKHOUSE_CLIENT -q "INSERT INTO ${ORD}.g VALUES ('b', 2, 0)" < /dev/null
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE ${ORD}.g FINAL" && echo 'merge ok'
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${ORD}.g"

echo 'D6 an already-unevaluable index of the same name is not re-checked'
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${ORD}.g2 (c0 String, c1 Int8, INDEX i0 c0 = c1 TYPE set(0)) ENGINE = MergeTree ORDER BY c1"
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.g2 DROP INDEX i0, ADD INDEX i0 c0 = c1 TYPE minmax" && echo ok
$CLICKHOUSE_CLIENT -q "SELECT type FROM system.data_skipping_indices WHERE database = '${ORD}' AND table = 'g2' AND name = 'i0'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.g2 ADD COLUMN x UInt8" && echo ok

echo 'D7 renaming a column the index references is still allowed'
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${ORD}.g3 (c0 String, c1 Int8, INDEX i0 c0 = c1 TYPE set(0)) ENGINE = MergeTree ORDER BY c1"
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.g3 RENAME COLUMN c0 TO c2" && echo ok
$CLICKHOUSE_CLIENT -q "SELECT expr FROM system.data_skipping_indices WHERE database = '${ORD}' AND table = 'g3' AND name = 'i0'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.g3 DROP INDEX i0" && echo ok

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${ORD} SYNC"

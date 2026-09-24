#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db1="${CLICKHOUSE_DATABASE}_src1"
db2="${CLICKHOUSE_DATABASE}_src2"
ov="${CLICKHOUSE_DATABASE}_ov"
user="u_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${ov}; DROP DATABASE IF EXISTS ${db1}; DROP DATABASE IF EXISTS ${db2}; DROP USER IF EXISTS ${user}"
}
trap cleanup EXIT

function error()
{
    "$@" 2>&1 | grep -o -m1 -E "ACCESS_DENIED|UNKNOWN_DATABASE|UNKNOWN_TABLE|BAD_ARGUMENTS|NOT_IMPLEMENTED|TABLE_IS_READ_ONLY|NUMBER_OF_ARGUMENTS_DOESNT_MATCH|SYNTAX_ERROR" || echo "no error"
}

$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE ${db1};
    CREATE DATABASE ${db2};
    CREATE TABLE ${db1}.t (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ${db2}.t (s String) ENGINE = MergeTree ORDER BY s;
    CREATE TABLE ${db2}.only2 (y UInt32) ENGINE = Memory;
    INSERT INTO ${db1}.t VALUES (1), (2), (3);
    INSERT INTO ${db2}.t VALUES ('shadowed');
    INSERT INTO ${db2}.only2 VALUES (42);
    CREATE DATABASE ${ov} ENGINE = Overlay(${db1}, '${db2}');
"

echo "--- listing, the first source wins"
$CLICKHOUSE_CLIENT -q "SELECT name, engine FROM system.tables WHERE database = '${ov}' ORDER BY name"
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${ov}.t ORDER BY x"
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${ov}.only2"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${ov}.t WHERE x > 1"

echo "--- insert goes to the source table"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${ov}.t VALUES (4)"
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM ${db1}.t"

echo "--- create queries"
$CLICKHOUSE_CLIENT -q "SHOW CREATE DATABASE ${ov}" | sed "s/${CLICKHOUSE_DATABASE}/db/g"
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE ${ov}.t" | sed "s/${CLICKHOUSE_DATABASE}/db/g"
$CLICKHOUSE_CLIENT -q "DESCRIBE TABLE ${ov}.t"

echo "--- DDL inside the overlay database is not supported"
error $CLICKHOUSE_CLIENT -q "CREATE TABLE ${ov}.new (x UInt8) ENGINE = Memory"
error $CLICKHOUSE_CLIENT -q "DROP TABLE ${ov}.t"
error $CLICKHOUSE_CLIENT -q "RENAME TABLE ${ov}.t TO ${ov}.t2"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${db1}.t"

echo "--- wrong arguments"
error $CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_bad ENGINE = Overlay"
error $CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_bad ENGINE = Overlay(${CLICKHOUSE_DATABASE}_missing)"
error $CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_bad ENGINE = Overlay(${ov})"

echo "--- sources are resolved by name"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${db2}"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = '${ov}' ORDER BY name"
error $CLICKHOUSE_CLIENT -q "SELECT * FROM ${ov}.only2"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${db2}; CREATE TABLE ${db2}.only2 (y UInt32) ENGINE = Memory; INSERT INTO ${db2}.only2 VALUES (43)"
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${ov}.only2"

echo "--- detach and attach"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${ov}"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${ov}"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${ov}.t"

echo "--- access requires grants on both the overlay database and the source table"
$CLICKHOUSE_CLIENT -q "CREATE USER ${user} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SELECT, INSERT ON ${ov}.* TO ${user}"
error $CLICKHOUSE_CLIENT --user "${user}" -q "SELECT * FROM ${ov}.t"
error $CLICKHOUSE_CLIENT --user "${user}" -q "INSERT INTO ${ov}.t VALUES (5)"
$CLICKHOUSE_CLIENT -q "REVOKE ALL ON ${ov}.* FROM ${user}; GRANT SELECT ON ${db1}.t TO ${user}"
error $CLICKHOUSE_CLIENT --user "${user}" -q "SELECT * FROM ${ov}.t"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${ov}.* TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT sum(x) FROM ${ov}.t"

echo "--- row policies of the source table apply"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p_${CLICKHOUSE_DATABASE} ON ${db1}.t USING x > 2 TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT * FROM ${ov}.t ORDER BY x"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p_${CLICKHOUSE_DATABASE} ON ${db1}.t"

echo "--- clickhouse-local keeps the actual table engines in its default database"
$CLICKHOUSE_LOCAL -q "CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x; SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't'"

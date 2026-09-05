#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names: a qualified name `a.b.c` is the table `c` of the database `a.b`, the table `b.c` of the
# database `a`, or the table `a.b.c` of the current database, whichever exists. `USE a.b` selects a database,
# or a namespace of tables `b.*` of the database `a`, or the databases `a.b.*`.

db=$CLICKHOUSE_DATABASE

# Strip the test database name, which varies between runs, out of the output.
function run()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | sed "s/${db}/db/g"
}

echo '--- namespace of tables in a regular database'
run "CREATE TABLE ${db}.\"sales.orders\" (id UInt64, x String) ENGINE = MergeTree ORDER BY id"
run "INSERT INTO ${db}.sales.orders VALUES (1, 'a'), (2, 'b')"
run "SELECT * FROM ${db}.sales.orders ORDER BY id"
run "SELECT id FROM sales.orders ORDER BY id"
run "SELECT sales.orders.id, ${db}.sales.orders.x FROM sales.orders ORDER BY id"
run "SELECT o.id, o.x FROM sales.orders AS o ORDER BY o.id"
run "SELECT * FROM \"${db}.sales\".orders ORDER BY id"
run "SELECT * FROM ${db}.sales.orders ORDER BY id SETTINGS enable_analyzer = 0"

echo '--- creating a table in an existing namespace, but not in a new one'
run "CREATE TABLE sales.customers (id UInt64) ENGINE = MergeTree ORDER BY id"
run "INSERT INTO sales.customers VALUES (2), (3)"
run "SELECT id FROM sales.orders WHERE id IN ${db}.sales.customers ORDER BY id"
run "SELECT id FROM sales.orders WHERE id IN sales.customers ORDER BY id"
run "SHOW CREATE TABLE sales.customers" | sed 's/ENGINE.*//'
run "CREATE TABLE marketing.leads (id UInt64) ENGINE = Memory" | grep -o 'UNKNOWN_DATABASE' | sort -u
run "CREATE TABLE ${db}.marketing.leads (id UInt64) ENGINE = Memory" | grep -o 'UNKNOWN_DATABASE' | sort -u
run "SHOW TABLES"
run "SHOW TABLES LIKE 'sales.%'"
run "SHOW FULL TABLES FROM ${db} WHERE name = 'sales.orders'"
run "EXISTS TABLE ${db}.sales.orders"
run "EXISTS TABLE sales.orders"
run "EXISTS TABLE sales.nothing"
run "DESCRIBE ${db}.sales.orders"
run "SELECT o.id, c.id FROM sales.orders AS o INNER JOIN sales.customers AS c ON o.id = c.id"
run "SELECT sales.orders.id, sales.customers.id FROM sales.orders INNER JOIN sales.customers ON sales.orders.id = sales.customers.id"

echo '--- DDL with hierarchical names'
run "ALTER TABLE sales.customers ADD COLUMN name String DEFAULT 'n'"
run "SELECT * FROM sales.customers ORDER BY id"
run "OPTIMIZE TABLE ${db}.sales.orders FINAL"
run "RENAME TABLE sales.customers TO sales.clients"
run "SELECT count() FROM sales.clients"
run "TRUNCATE TABLE sales.clients"
run "SELECT count() FROM sales.clients"
run "CREATE TABLE sales.clients_copy AS ${db}.sales.clients"
run "SHOW TABLES LIKE 'sales.clients%'"
run "DROP TABLE sales.clients_copy"
run "DROP TABLE ${db}.sales.clients"
run "SHOW TABLES"

echo '--- errors'
run "SELECT * FROM ${db}.sales.nothing" | grep -o 'UNKNOWN_TABLE' | sort -u
run "SELECT * FROM sales.nothing" | grep -o 'UNKNOWN_TABLE' | sort -u
run "SELECT * FROM nodb.t" | grep -o 'UNKNOWN_DATABASE' | sort -u
run "USE ${db}.nothing" | grep -o 'UNKNOWN_DATABASE' | sort -u

echo '--- USE with a namespace'
$CLICKHOUSE_CLIENT -m -q "
USE ${db}.sales;
SELECT currentDatabase() = '${db}.sales';
SELECT count() FROM orders;
SELECT orders.id, sales.orders.x FROM orders ORDER BY id;
CREATE TABLE returns (id UInt64) ENGINE = Memory;
INSERT INTO returns VALUES (5), (1);
SELECT * FROM returns ORDER BY id;
SELECT id FROM ${db}.sales.orders WHERE id IN returns ORDER BY id;
SELECT * FROM ${db}.sales.returns;
SHOW TABLES;
SHOW TABLES FROM ${db}.sales;
EXISTS TABLE returns;
DESCRIBE returns;
CREATE MATERIALIZED VIEW returns_mv TO returns AS SELECT id FROM orders;
INSERT INTO orders VALUES (9, 'c');
SELECT * FROM returns ORDER BY id;
SELECT * FROM orders ORDER BY id;
SHOW TABLES;
DROP TABLE returns_mv;
DROP TABLE returns;
SHOW TABLES;
" 2>&1 | sed "s/${db}/db/g"

echo '--- the database parameter of HTTP with a namespace'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&database=${db}.sales" -d "SELECT count() FROM orders"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&database=${db}.sales" -d "SHOW TABLES"

echo '--- databases with dots in their names'
run "CREATE DATABASE ${db}.sub"
run "CREATE TABLE ${db}.sub.t (id UInt64) ENGINE = Memory"
run "INSERT INTO ${db}.sub.t VALUES (7)"
run "SELECT * FROM ${db}.sub.t"
run "SELECT ${db}.sub.t.id FROM ${db}.sub.t"
run "SELECT * FROM sub.t"
run "SHOW TABLES FROM ${db}"
run "SHOW TABLES FROM ${db}.sub"
run "EXISTS TABLE sub.t"
$CLICKHOUSE_CLIENT -m -q "
USE ${db}.sub;
SELECT * FROM t;
SHOW TABLES;
" 2>&1 | sed "s/${db}/db/g"

echo '--- the longest database wins over a namespace'
run "CREATE TABLE ${db}.\"sub.t\" (id UInt64) ENGINE = Memory"
run "INSERT INTO ${db}.\"sub.t\" VALUES (8)"
run "SELECT * FROM ${db}.sub.t"
run "SELECT * FROM sub.t"
run "SELECT * FROM ${db}.\"sub.t\""
run "SHOW TABLES FROM ${db}"
run "DROP TABLE ${db}.\"sub.t\""
run "DROP DATABASE ${db}.sub"

echo '--- USE of a common prefix of database names'
run "CREATE DATABASE \"${db}_p.a\""
run "CREATE TABLE \"${db}_p.a\".t (id UInt64) ENGINE = Memory"
run "INSERT INTO ${db}_p.a.t VALUES (10)"
$CLICKHOUSE_CLIENT -m -q "
USE ${db}_p;
SELECT currentDatabase() = '${db}_p';
SELECT * FROM a.t;
SELECT a.t.id FROM a.t;
SHOW TABLES;
" 2>&1 | sed "s/${db}/db/g"
run "DROP DATABASE ${db}_p.a"

run "DROP TABLE ${db}.sales.orders"
run "SHOW TABLES"

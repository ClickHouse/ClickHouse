#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the SQLite library

# https://github.com/ClickHouse/ClickHouse/issues/116799
# A filter on the columns of an external table must stay in ClickHouse when the table is on the non-preserving
# side of an outer join, wherever that join sits in the join tree, or on a side a join picks one row per key
# from: filtered before the join, the rows the filter rejects turn into unmatched or differently chosen rows.
# Every query runs against the SQLite table and then against a Memory copy of it; the results must agree.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.sqlite3"
rm -f "${DB_PATH}"

sqlite3 "${DB_PATH}" "
CREATE TABLE products (id INTEGER PRIMARY KEY, description TEXT);
INSERT INTO products VALUES (1, 'one'), (2, 'two');
CREATE TABLE prices (product_id INTEGER, since INTEGER, price INTEGER);
INSERT INTO prices VALUES (1, 0, 10), (1, 3, 20);
CREATE TABLE flags (k INTEGER PRIMARY KEY, f INTEGER);
INSERT INTO flags VALUES (1, 0), (2, 1);
"

${CLICKHOUSE_LOCAL} --multiquery "
CREATE TABLE orders (id Int64, product_id Int64, t Int64) ENGINE = Memory;
INSERT INTO orders VALUES (1, 1, 5), (2, 2, 5), (3, 3, 5), (4, 4, 5), (5, 1, 2);
CREATE TABLE products (id Int64, description Nullable(String)) ENGINE = SQLite('${DB_PATH}', 'products');
CREATE TABLE prices (product_id Int64, since Int64, price Int64) ENGINE = SQLite('${DB_PATH}', 'prices');
CREATE TABLE products_memory ENGINE = Memory AS SELECT * FROM products;
CREATE TABLE prices_memory ENGINE = Memory AS SELECT * FROM prices;
CREATE TABLE flags (k Int64, f Int64) ENGINE = SQLite('${DB_PATH}', 'flags');
CREATE TABLE flags_memory ENGINE = Memory AS SELECT * FROM flags;
CREATE TABLE local_flags (k Int64, f Int64) ENGINE = Memory;
INSERT INTO local_flags VALUES (1, 1), (2, 0);

SELECT '-- LEFT JOIN, then CROSS JOIN';
SELECT o.id FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description IS NULL ORDER BY o.id;
SELECT o.id FROM orders AS o LEFT JOIN products_memory AS p ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description IS NULL ORDER BY o.id;

SELECT '-- LEFT JOIN, then comma join';
SELECT o.id FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id, (SELECT 1) AS x WHERE p.description IS NULL ORDER BY o.id;
SELECT o.id FROM orders AS o LEFT JOIN products_memory AS p ON o.product_id = p.id, (SELECT 1) AS x WHERE p.description IS NULL ORDER BY o.id;

SELECT '-- LEFT JOIN, then INNER JOIN';
SELECT o.id FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id INNER JOIN (SELECT 1 AS k) AS x ON x.k = 1 WHERE p.description IS NULL ORDER BY o.id;
SELECT o.id FROM orders AS o LEFT JOIN products_memory AS p ON o.product_id = p.id INNER JOIN (SELECT 1 AS k) AS x ON x.k = 1 WHERE p.description IS NULL ORDER BY o.id;

SELECT '-- LEFT JOIN, then ARRAY JOIN';
SELECT o.id FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id ARRAY JOIN [1] AS arr WHERE p.description IS NULL ORDER BY o.id;
SELECT o.id FROM orders AS o LEFT JOIN products_memory AS p ON o.product_id = p.id ARRAY JOIN [1] AS arr WHERE p.description IS NULL ORDER BY o.id;

SELECT '-- two LEFT JOINs, then CROSS JOIN, filter on the second';
SELECT o.id FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id LEFT JOIN prices AS pr ON pr.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE pr.price IS NULL ORDER BY o.id SETTINGS join_use_nulls = 1;
SELECT o.id FROM orders AS o LEFT JOIN products_memory AS p ON o.product_id = p.id LEFT JOIN prices_memory AS pr ON pr.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE pr.price IS NULL ORDER BY o.id SETTINGS join_use_nulls = 1;

SELECT '-- two LEFT JOINs, then CROSS JOIN, filter on the first';
SELECT o.id FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id LEFT JOIN prices AS pr ON pr.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description IS NULL ORDER BY o.id;
SELECT o.id FROM orders AS o LEFT JOIN products_memory AS p ON o.product_id = p.id LEFT JOIN prices_memory AS pr ON pr.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description IS NULL ORDER BY o.id;

SELECT '-- RIGHT JOIN, then CROSS JOIN';
SELECT o.id FROM products AS p RIGHT JOIN orders AS o ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description IS NULL ORDER BY o.id;
SELECT o.id FROM products_memory AS p RIGHT JOIN orders AS o ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description IS NULL ORDER BY o.id;

SELECT '-- FULL JOIN, then CROSS JOIN';
SELECT o.id FROM orders AS o FULL JOIN products AS p ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description IS NULL ORDER BY o.id;
SELECT o.id FROM orders AS o FULL JOIN products_memory AS p ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description IS NULL ORDER BY o.id;

SELECT '-- LEFT ANY JOIN, then CROSS JOIN';
SELECT o.id FROM orders AS o LEFT ANY JOIN products AS p ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description IS NULL ORDER BY o.id;
SELECT o.id FROM orders AS o LEFT ANY JOIN products_memory AS p ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description IS NULL ORDER BY o.id;

SELECT '-- the default value of an unmatched non-Nullable column';
SELECT o.id FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.id = 0 ORDER BY o.id SETTINGS join_use_nulls = 0;
SELECT o.id FROM orders AS o LEFT JOIN products_memory AS p ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.id = 0 ORDER BY o.id SETTINGS join_use_nulls = 0;

SELECT '-- ASOF JOIN: the closest row must be chosen among all rows';
SELECT o.id FROM orders AS o ASOF JOIN prices AS pr ON o.product_id = pr.product_id AND o.t >= pr.since WHERE pr.price = 10 ORDER BY o.id;
SELECT o.id FROM orders AS o ASOF JOIN prices_memory AS pr ON o.product_id = pr.product_id AND o.t >= pr.since WHERE pr.price = 10 ORDER BY o.id;

SELECT '-- the preserving side of a nested outer join';
SELECT p.id FROM products AS p LEFT JOIN orders AS o ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description = 'one' ORDER BY p.id;
SELECT p.id FROM products_memory AS p LEFT JOIN orders AS o ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description = 'one' ORDER BY p.id;

SELECT '-- a bare column of the joined table is not sent to SQLite, wherever the join sits';
SELECT count() FROM flags AS a LEFT JOIN local_flags AS b ON a.k = b.k CROSS JOIN (SELECT 1) AS x WHERE b.f;
SELECT count() FROM flags_memory AS a LEFT JOIN local_flags AS b ON a.k = b.k CROSS JOIN (SELECT 1) AS x WHERE b.f;
SELECT count() FROM flags AS a LEFT JOIN local_flags AS b ON a.k = b.k LEFT JOIN local_flags AS c ON a.k = c.k WHERE b.f;
SELECT count() FROM flags_memory AS a LEFT JOIN local_flags AS b ON a.k = b.k LEFT JOIN local_flags AS c ON a.k = c.k WHERE b.f;

SELECT '-- external_table_strict_query: nothing is pushed down from the non-preserving side, so nothing is rejected';
SELECT count() FROM orders AS o LEFT JOIN products AS p ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE ifNull(p.description, '') = '' SETTINGS external_table_strict_query = 1;
SELECT count() FROM orders AS o LEFT JOIN sqlite('${DB_PATH}', query('SELECT id, description FROM products')) AS p ON o.product_id = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description IS NULL SETTINGS external_table_strict_query = 1;
"

echo '-- the query sent to SQLite'
${CLICKHOUSE_LOCAL} --send_logs_level=trace --multiquery "
SELECT count() FROM numbers(4) AS o LEFT JOIN sqlite('${DB_PATH}', 'products') AS p ON o.number = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description IS NULL FORMAT Null;
SELECT count() FROM sqlite('${DB_PATH}', 'products') AS p LEFT JOIN numbers(4) AS o ON o.number = p.id CROSS JOIN (SELECT 1) AS x WHERE p.description = 'one' FORMAT Null;
SELECT count() FROM sqlite('${DB_PATH}', 'flags') AS a LEFT JOIN (SELECT 1 AS k, 1 AS f) AS b ON a.k = b.k LEFT JOIN (SELECT 1 AS k) AS c ON a.k = c.k WHERE b.f FORMAT Null;
SELECT count() FROM sqlite('${DB_PATH}', 'products') AS p ANY LEFT JOIN numbers(4) AS o ON o.number = p.id WHERE p.description = 'one' FORMAT Null;
" 2>&1 | grep -o 'StorageSQLite.*: Query: .*' | sed 's/^.*: Query: //'

rm -f "${DB_PATH}"

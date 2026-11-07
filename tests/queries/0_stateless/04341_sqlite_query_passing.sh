#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_TMP}/04341.sqlite3"
rm -f "$DB"

sqlite3 "$DB" 'CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT, val REAL);'
sqlite3 "$DB" "INSERT INTO t1 VALUES (1, 'a', 1.5), (2, 'b', 2.5), (3, 'c', 3.5), (4, 'd', 4.5);"
sqlite3 "$DB" 'CREATE TABLE t2(id INTEGER, category TEXT);'
sqlite3 "$DB" "INSERT INTO t2 VALUES (1, 'x'), (2, 'y'), (4, 'z');"
sqlite3 "$DB" 'CREATE TABLE t3("Mixed Id" INTEGER, val TEXT);'
sqlite3 "$DB" "INSERT INTO t3 VALUES (7, 'seven');"
sqlite3 "$DB" 'CREATE TABLE t_esc(id INTEGER PRIMARY KEY, s TEXT);'
sqlite3 "$DB" "INSERT INTO t_esc VALUES (1, 'it''s'), (2, 'a' || char(9) || 'b');"

${CLICKHOUSE_LOCAL} --multiquery "
SELECT '-- baseline: table name still works';
SELECT count() FROM sqlite('${DB}', 't1');

SELECT '-- query(...) form: filtering is done on the SQLite side';
SELECT count() FROM sqlite('${DB}', query('SELECT * FROM t1 WHERE id % 2 = 0'));

SELECT '-- subquery form';
SELECT id, name FROM sqlite('${DB}', (SELECT id, name FROM t1 WHERE id > 2)) ORDER BY id;

SELECT '-- schema inference from a passthrough query';
DESCRIBE TABLE sqlite('${DB}', query('SELECT id, name, val FROM t1'));

SELECT '-- a JOIN passed to SQLite as is';
SELECT id, name, category
FROM sqlite('${DB}', query('SELECT t1.id AS id, t1.name AS name, t2.category AS category FROM t1 JOIN t2 ON t1.id = t2.id'))
ORDER BY id;

SELECT '-- engine with a query, reading and write rejection';
CREATE TABLE engine_query ENGINE = SQLite('${DB}', query('SELECT id, name FROM t1 WHERE id <= 2'));
SELECT * FROM engine_query ORDER BY id;
INSERT INTO engine_query VALUES (5, 'e'); -- { serverError INCORRECT_QUERY }
DROP TABLE engine_query;

SELECT '-- subquery form quotes identifiers in the target dialect';
SELECT \"Mixed Id\", val FROM sqlite('${DB}', (SELECT \"Mixed Id\", val FROM t3));

SELECT '-- external_table_strict_query: an outer filter is applied locally by default';
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) WHERE id = 1;

SELECT '-- external_table_strict_query: an outer filter that cannot be pushed down is rejected';
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) WHERE id = 1 SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }

SELECT '-- external_table_strict_query: no outer filter is allowed';
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) SETTINGS external_table_strict_query = 1;

-- The verdicts below hold on the analyzer path only: with enable_analyzer = 0 the guard
-- inspects the original AST and rejects every one of them.
SET enable_analyzer = 1;

SELECT '-- external_table_strict_query: a predicate on the joined local side is not a filter on the source';
-- The flag column is absent from the passed query, so it cannot be confused with a source column.
CREATE TABLE local_r (id Int64, flag UInt8) ENGINE = Memory;
INSERT INTO local_r VALUES (1, 1), (2, 0);
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE r.flag SETTINGS external_table_strict_query = 1;
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE r.flag = 1 SETTINGS external_table_strict_query = 1;
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE r.id SETTINGS external_table_strict_query = 1;
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE r.id = 1 SETTINGS external_table_strict_query = 1;

SELECT '-- external_table_strict_query: a source predicate at the top level or as a conjunct is rejected';
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE l.id SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE l.id = 1 SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE l.id = 1 AND r.id SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }

-- A disjunction mixing the two sides is dropped whole, so the guard sees no filter and does
-- not fire. The counts must agree: this is a missed rejection, not a wrong answer.
SELECT '-- external_table_strict_query: a disjunction mixing the source and the local side is not rejected';
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE l.id = 1 OR r.flag SETTINGS external_table_strict_query = 1;
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE l.id = 1 OR r.flag SETTINGS external_table_strict_query = 0;

-- On the non-preserving side of an outer join (and on either side of a FULL JOIN) no filter is
-- pushed down at all, so the guard sees no filter and a source predicate is accepted. The strict
-- and non-strict counts must agree: this is a missed rejection, not a wrong answer.
SELECT '-- external_table_strict_query: a source predicate on the non-preserving side of an outer join is not checked';
SELECT count() FROM local_r AS l LEFT JOIN sqlite('${DB}', query('SELECT id, name FROM t1')) AS r USING (id) WHERE r.id SETTINGS external_table_strict_query = 1;
SELECT count() FROM local_r AS l LEFT JOIN sqlite('${DB}', query('SELECT id, name FROM t1')) AS r USING (id) WHERE r.id = 1 SETTINGS external_table_strict_query = 1;
SELECT count() FROM local_r AS l LEFT JOIN sqlite('${DB}', query('SELECT id, name FROM t1')) AS r USING (id) WHERE r.id = 1 SETTINGS external_table_strict_query = 0;
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l RIGHT JOIN local_r AS r USING (id) WHERE l.id = 1 SETTINGS external_table_strict_query = 1;
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l FULL JOIN local_r AS r USING (id) WHERE l.id = 1 SETTINGS external_table_strict_query = 1;
SELECT count() FROM local_r AS l FULL JOIN sqlite('${DB}', query('SELECT id, name FROM t1')) AS r USING (id) WHERE r.id = 1 SETTINGS external_table_strict_query = 1;

SELECT '-- external_table_strict_query: the same source predicate on the preserving side or in an inner join is rejected';
SELECT count() FROM local_r AS l RIGHT JOIN sqlite('${DB}', query('SELECT id, name FROM t1')) AS r USING (id) WHERE r.id = 1 SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }
SELECT count() FROM local_r AS l INNER JOIN sqlite('${DB}', query('SELECT id, name FROM t1')) AS r USING (id) WHERE r.id = 1 SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }
DROP TABLE local_r;

-- A PREWHERE on a joined table that supports it is never a filter on the source. On the
-- non-preserving side (and in a FULL JOIN) nothing is pushed down, so the reconstructed query
-- must drop it together with WHERE instead of presenting it to the guard as a source filter.
SELECT '-- external_table_strict_query: a PREWHERE on the joined local side is not a filter on the source';
CREATE TABLE local_mt (id Int64, flag UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO local_mt VALUES (1, 1), (2, 0);
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_mt AS r USING (id) PREWHERE r.flag SETTINGS external_table_strict_query = 1;
SELECT count() FROM local_mt AS l LEFT JOIN sqlite('${DB}', query('SELECT id, name FROM t1')) AS r USING (id) PREWHERE l.flag SETTINGS external_table_strict_query = 1;
SELECT count() FROM local_mt AS l LEFT JOIN sqlite('${DB}', query('SELECT id, name FROM t1')) AS r USING (id) PREWHERE l.flag SETTINGS external_table_strict_query = 0;
SELECT count() FROM local_mt AS l FULL JOIN sqlite('${DB}', query('SELECT id, name FROM t1')) AS r USING (id) PREWHERE l.flag = 1 SETTINGS external_table_strict_query = 1;
SELECT count() FROM local_mt AS l LEFT JOIN sqlite('${DB}', 't1') AS r USING (id) PREWHERE l.flag SETTINGS external_table_strict_query = 1;

SELECT '-- external_table_strict_query: a PREWHERE on the SQLite table itself is rejected by the engine, not by the setting';
SELECT count() FROM local_mt AS l LEFT JOIN sqlite('${DB}', query('SELECT id, name FROM t1')) AS r USING (id) PREWHERE r.id = 1 SETTINGS external_table_strict_query = 0; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM sqlite('${DB}', 't1') PREWHERE id = 1 SETTINGS external_table_strict_query = 0; -- { serverError ILLEGAL_PREWHERE }
DROP TABLE local_mt;

SELECT '-- INSERT into a query-backed table function is rejected before schema inference';
INSERT INTO TABLE FUNCTION sqlite('${DB}', query('SELECT id FROM nonexistent_table')) VALUES (1); -- { serverError INCORRECT_QUERY }

SELECT '-- projection-count mismatch: an explicit structure with more columns than the query pads with defaults';
CREATE TABLE count_mismatch (id Int64, name String, extra Int32) ENGINE = SQLite('${DB}', query('SELECT id, name FROM t1'));
SELECT * FROM count_mismatch ORDER BY id;
DROP TABLE count_mismatch;

SELECT '-- type mismatch: a text value read into a declared Date is a query error, not a crash';
CREATE TABLE type_mismatch (id Int64, name Date) ENGINE = SQLite('${DB}', query('SELECT id, name FROM t1'));
SELECT * FROM type_mismatch; -- { serverError CANNOT_PARSE_DATE }
DROP TABLE type_mismatch;

SELECT '-- subquery form: special-character string literals are escaped for SQLite';
SELECT id FROM sqlite('${DB}', (SELECT id FROM t_esc WHERE s = 'it''s')) ORDER BY id;
SELECT id FROM sqlite('${DB}', (SELECT id FROM t_esc WHERE s = 'a\tb')) ORDER BY id;
"

rm -f "$DB"

-- `optimize_push_subcolumns_into_subqueries`: a storage that can read only tuple elements (`file`) gets a named
-- tuple element pushed into it, but no other subcolumn, and a `UNION` source is left alone.

SET enable_analyzer = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05325.tsv', 'TSV', 'tup Tuple(a String, b Int32), arr Array(UInt32), ntup Nullable(Tuple(a String)), utup Tuple(String, Int32)')
SETTINGS engine_file_truncate_on_insert = 1 VALUES (('x', 1), [1, 2], ('p'), ('u', 1)), (('y', 2), [3], NULL, ('v', 2));

SELECT 'named tuple element';
SELECT countIf(explain LIKE '%column_name: tup.a,%'), countIf(explain LIKE '%column_name: tup,%')
FROM (EXPLAIN QUERY TREE SELECT tup.a FROM (SELECT tup FROM file(currentDatabase() || '_05325.tsv', 'TSV', 'tup Tuple(a String, b Int32), arr Array(UInt32), ntup Nullable(Tuple(a String)), utup Tuple(String, Int32)')));
SELECT tup.a FROM (SELECT tup FROM file(currentDatabase() || '_05325.tsv', 'TSV', 'tup Tuple(a String, b Int32), arr Array(UInt32), ntup Nullable(Tuple(a String)), utup Tuple(String, Int32)')) ORDER BY ALL;
SELECT tup.a FROM (SELECT tup FROM file(currentDatabase() || '_05325.tsv', 'TSV', 'tup Tuple(a String, b Int32), arr Array(UInt32), ntup Nullable(Tuple(a String)), utup Tuple(String, Int32)')) ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'array sizes, nullable and unnamed tuples are not pushed';
SELECT countIf(explain LIKE '%column_name: arr.size0,%'), countIf(explain LIKE '%column_name: ntup.a,%'), countIf(explain LIKE '%column_name: utup.1,%')
FROM (EXPLAIN QUERY TREE SELECT arr.size0, ntup.a, utup.1 FROM (SELECT arr, ntup, utup FROM file(currentDatabase() || '_05325.tsv', 'TSV', 'tup Tuple(a String, b Int32), arr Array(UInt32), ntup Nullable(Tuple(a String)), utup Tuple(String, Int32)')));
SELECT arr.size0, ntup.a, utup.1 FROM (SELECT arr, ntup, utup FROM file(currentDatabase() || '_05325.tsv', 'TSV', 'tup Tuple(a String, b Int32), arr Array(UInt32), ntup Nullable(Tuple(a String)), utup Tuple(String, Int32)')) ORDER BY ALL;
SELECT arr.size0, ntup.a, utup.1 FROM (SELECT arr, ntup, utup FROM file(currentDatabase() || '_05325.tsv', 'TSV', 'tup Tuple(a String, b Int32), arr Array(UInt32), ntup Nullable(Tuple(a String)), utup Tuple(String, Int32)')) ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

DROP TABLE IF EXISTS t_05325_1;
DROP TABLE IF EXISTS t_05325_2;
CREATE TABLE t_05325_1 (tup Tuple(a String, b Int32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_05325_2 (tup Tuple(a String, b Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05325_1 VALUES (('x', 1));
INSERT INTO t_05325_2 VALUES (('y', 2));

SELECT 'union source';
WITH u AS ((SELECT tup FROM t_05325_1) UNION ALL (SELECT tup FROM t_05325_2))
SELECT tup.a FROM u ORDER BY ALL;
WITH u AS ((SELECT tup FROM t_05325_1) UNION ALL (SELECT tup FROM t_05325_2))
SELECT tup.a FROM u ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;
SELECT tup.a FROM (SELECT tup FROM ((SELECT tup FROM t_05325_1) UNION ALL (SELECT tup FROM t_05325_2))) ORDER BY ALL;

DROP TABLE t_05325_1;
DROP TABLE t_05325_2;

-- A correlated subquery that reads a tuple element of an outer column through its position -
-- `o.p.1`, which is `tupleElement(o.p, 1)` - lists the outer column among its correlated columns.
-- `optimize_functions_to_subcolumns` replaced that function with a subcolumn of the column, leaving
-- the list naming a column the subquery no longer reads, and decorrelation then looked for the
-- subcolumn in a join carrying the whole column: `NOT_FOUND_COLUMN_IN_BLOCK`.

SET allow_experimental_correlated_subqueries = 1;
-- The bug needs the optimization that rewrites the function into a subcolumn read, and the last
-- assertion below reads its plan, so pin it rather than take the randomized value.
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_correlated_tuple;
CREATE TABLE t_correlated_tuple (p Tuple(a Int32, b String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_correlated_tuple VALUES ((1, 'x')), ((2, 'y'));

SELECT 'a positional element of the outer column';
SELECT count() FROM t_correlated_tuple AS o WHERE EXISTS (SELECT 1 FROM t_correlated_tuple AS i WHERE i.p.a = o.p.1);
SELECT count() FROM t_correlated_tuple AS o WHERE EXISTS (SELECT 1 FROM t_correlated_tuple AS i WHERE i.p.a = tupleElement(o.p, 1));

SELECT 'by name, which always worked';
SELECT count() FROM t_correlated_tuple AS o WHERE EXISTS (SELECT 1 FROM t_correlated_tuple AS i WHERE i.p.a = o.p.a);

SELECT 'a correlated scalar subquery';
SELECT count() FROM t_correlated_tuple AS o WHERE 1 = (SELECT count() FROM t_correlated_tuple AS i WHERE i.p.1 = o.p.1);

SELECT 'the same element on both sides';
SELECT o.p.1 FROM t_correlated_tuple AS o WHERE EXISTS (SELECT 1 FROM t_correlated_tuple AS i WHERE i.p.1 = o.p.1) ORDER BY o.p.1;

-- The `IN (subquery)` to join rewrite builds such a correlated subquery itself.
SELECT 'the IN to join rewrite of a positional element';
SELECT p.2 FROM t_correlated_tuple WHERE p.2 IN (SELECT 'x') SETTINGS rewrite_in_to_join = 1;
SELECT p.2 FROM t_correlated_tuple WHERE p.2 NOT IN (SELECT 'x') SETTINGS rewrite_in_to_join = 1;
SELECT p.2 FROM t_correlated_tuple WHERE tupleElement(p, 2) IN (SELECT 'x') SETTINGS rewrite_in_to_join = 1;

-- A tuple element of a column that no subquery reads from the outside is still turned into a
-- subcolumn read.
SELECT 'the optimization still applies outside a correlated subquery';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT p.1 FROM t_correlated_tuple) WHERE explain LIKE '%column_name: p.a%';

DROP TABLE t_correlated_tuple;

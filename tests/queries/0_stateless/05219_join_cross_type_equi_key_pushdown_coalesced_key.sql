-- Pushing an equi-key `WHERE` predicate through a cross-type `JOIN` substitutes the opposite side's key
-- for the predicate's column. When that key is the coalesced `USING` column of a preceding `FULL JOIN` it
-- is an expression the `JOIN` computes, not a column of the stream the pushed-down filter reads, so it has
-- to be materialised into that filter instead of being referenced by name.
--
-- Analyzer only: the substitution is keyed by the analyzer's `__tableN.k` names.

SET enable_analyzer = 1;
SET join_use_nulls = 0;
SET query_plan_filter_push_down = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_join_swap_table = 'false';
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;
DROP TABLE IF EXISTS bn;

CREATE TABLE a  (k UInt8)            ENGINE = Memory;
CREATE TABLE b  (k UInt64)           ENGINE = Memory;
CREATE TABLE c  (k UInt16)           ENGINE = Memory;
CREATE TABLE bn (k Nullable(UInt64)) ENGINE = Memory;

SELECT 'FULL + LEFT JOIN USING, cross-type keys, empty tables';
SELECT a.k, b.k, c.k FROM a FULL JOIN b USING (k) LEFT JOIN c USING (k) WHERE c.k > 1 ORDER BY ALL;

SELECT 'FULL + RIGHT JOIN USING, cross-type keys, empty tables';
SELECT a.k, b.k, c.k FROM a FULL JOIN b USING (k) RIGHT JOIN c USING (k) WHERE c.k > 1 ORDER BY ALL;

INSERT INTO a  VALUES (1), (2), (3);
INSERT INTO b  VALUES (2), (3), (4);
INSERT INTO c  VALUES (3), (4), (5);
INSERT INTO bn VALUES (2), (3), (4);

SELECT 'FULL + LEFT JOIN USING, cross-type keys: result';
SELECT a.k, b.k, c.k FROM a FULL JOIN b USING (k) LEFT JOIN c USING (k) WHERE c.k > 1 ORDER BY ALL;

SELECT 'FULL + LEFT JOIN USING, cross-type keys: the push-down does not change the result';
SELECT a.k, b.k, c.k FROM a FULL JOIN b USING (k) LEFT JOIN c USING (k) WHERE c.k > 1 ORDER BY ALL
SETTINGS query_plan_filter_push_down = 0;

SELECT 'FULL + RIGHT JOIN USING, cross-type keys: result';
SELECT a.k, b.k, c.k FROM a FULL JOIN b USING (k) RIGHT JOIN c USING (k) WHERE c.k > 1 ORDER BY ALL;

SELECT 'FULL + RIGHT JOIN USING, cross-type keys: the push-down does not change the result';
SELECT a.k, b.k, c.k FROM a FULL JOIN b USING (k) RIGHT JOIN c USING (k) WHERE c.k > 1 ORDER BY ALL
SETTINGS query_plan_filter_push_down = 0;

SELECT 'FULL + FULL JOIN USING, cross-type keys: result';
SELECT a.k, b.k, c.k FROM a FULL JOIN b USING (k) FULL JOIN c USING (k) WHERE b.k > 1 AND c.k > 1 ORDER BY ALL;

SELECT 'FULL + FULL JOIN USING, cross-type keys: the push-down does not change the result';
SELECT a.k, b.k, c.k FROM a FULL JOIN b USING (k) FULL JOIN c USING (k) WHERE b.k > 1 AND c.k > 1 ORDER BY ALL
SETTINGS query_plan_filter_push_down = 0;

SELECT 'FULL + LEFT JOIN USING, Nullable cross-type keys: result';
SELECT a.k, bn.k, c.k FROM a FULL JOIN bn USING (k) LEFT JOIN c USING (k) WHERE c.k > 1 ORDER BY ALL;

SELECT 'FULL + LEFT JOIN USING: the coalesced key is computed inside the pushed-down filter';
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1
    SELECT a.k, b.k, c.k FROM a FULL JOIN b USING (k) LEFT JOIN c USING (k) WHERE c.k > 1
) WHERE explain ILIKE '%Filter column: firstNonDefault(%) > 1%';

DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
DROP TABLE bn;

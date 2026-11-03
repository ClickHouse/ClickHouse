-- The right keys of a JOIN converted to IN become the source of the new set. A set inside them
-- must not be read before it is built: index analysis builds the new set synchronously.

SET enable_analyzer = 1;
SET query_plan_enable_optimizations = 1;
SET query_plan_convert_join_to_in = 1;
SET join_algorithm = 'hash';
SET use_index_for_in_with_subqueries = 1;
SET optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS t;

CREATE TABLE l (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO l VALUES (0), (1), (2);
CREATE TABLE r (k UInt64) ENGINE = Memory;
INSERT INTO r VALUES (0), (1), (2);
CREATE TABLE t (k UInt64, v UInt8) ENGINE = Memory;
INSERT INTO t VALUES (0, 0), (1, 0), (2, 0);

SELECT a.k FROM l AS a ANY INNER JOIN r AS b ON a.k = b.k + toUInt64(b.k IN (SELECT number FROM numbers(2))) ORDER BY a.k;

SELECT k, k IN (SELECT a.k FROM l AS a ANY INNER JOIN r AS b ON a.k = b.k + toUInt64(b.k IN (SELECT number FROM numbers(2))))
FROM t ORDER BY k;

ALTER TABLE t UPDATE v = k IN
(
    SELECT a.k FROM l AS a ANY INNER JOIN r AS b
    ON a.k = b.k + toUInt64(b.k IN (SELECT number FROM numbers(2)))
)
WHERE 1;

SELECT k, v FROM t ORDER BY k;

DROP TABLE l;
DROP TABLE r;
DROP TABLE t;

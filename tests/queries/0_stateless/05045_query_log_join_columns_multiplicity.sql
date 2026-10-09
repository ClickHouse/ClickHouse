-- Tags: no-old-analyzer

-- `used_join_kinds` and `used_join_strictness` describe every physical join of the pipeline, one array
-- element per join, so a query whose joins share a kind and a strictness reports that combination as
-- many times as it occurs and the arrays are as long as `used_number_of_joins`. The elements are
-- sorted, so that they do not depend on the order in which the joins were executed, and the two arrays
-- are positionally aligned: the element at a given index in both describes the same join.
--
-- The algorithm is set explicitly, because the choice among the algorithms allowed by `join_algorithm`
-- is made at run time and depends on the number of threads.

SET log_queries = 1;
-- The reported kind is the executed one, and the optimizer may execute a join with its sides swapped,
-- which reverses LEFT and RIGHT.
SET query_plan_join_swap_table = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;

CREATE TABLE t1 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t3 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t4 (a UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 SELECT number FROM numbers(10);
INSERT INTO t2 SELECT number FROM numbers(10);
INSERT INTO t3 SELECT number FROM numbers(10);
INSERT INTO t4 SELECT number FROM numbers(10);

SELECT 'three joins that share a kind and a strictness';
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t2.a = t3.a JOIN t4 ON t3.a = t4.a
FORMAT Null SETTINGS log_comment = '05045_join_multiplicity_a_same', join_algorithm = 'hash';

SELECT 'a repeated combination and a distinct one, in the same query';
SELECT count() FROM t1 ANY INNER JOIN t2 ON t1.a = t2.a ANY INNER JOIN t3 ON t2.a = t3.a
                     LEFT JOIN t4 ON t3.a = t4.a
FORMAT Null SETTINGS log_comment = '05045_join_multiplicity_b_mixed', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, used_number_of_joins, used_join_kinds, used_join_strictness,
       arrayZip(used_join_kinds, used_join_strictness) AS joins_as_pairs,
       length(used_join_kinds) = used_number_of_joins AND length(used_join_strictness) = used_number_of_joins AS arrays_hold_one_element_per_join
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05045\_join\_multiplicity\_%'
ORDER BY log_comment;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;

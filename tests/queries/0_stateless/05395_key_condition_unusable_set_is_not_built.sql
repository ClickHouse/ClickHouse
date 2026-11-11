-- `x` appears among the key subexpressions of `tuple(x, y)`, but no key column is computable from `x` alone,
-- so no atom can use the set of `x IN (subquery)`. Index analysis must not build that set, otherwise even
-- `EXPLAIN` executes the subquery. A key column that the predicate does reach builds the set, which shows
-- that the read rows observe building it.

SET log_queries = 1;
SET use_index_for_in_with_subqueries = 1;
SET analyze_index_with_multiple_key_columns_per_condition = 1;

CREATE TABLE t_set (x UInt64, y UInt64, z UInt64) ENGINE = MergeTree ORDER BY (tuple(x, y), z);
INSERT INTO t_set VALUES (1, 1, 1), (2, 2, 2);

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(x) FROM t_set WHERE x IN (SELECT number FROM numbers(1000000)))
SETTINGS log_comment = 'unusable set';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(x) FROM t_set WHERE z IN (SELECT number FROM numbers(1000000)))
SETTINGS log_comment = 'usable set';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_rows >= 1000000 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('unusable set', 'usable set') AND type = 'QueryFinish'
ORDER BY log_comment;

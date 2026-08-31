-- https://github.com/ClickHouse/ClickHouse/issues/116938
-- `optimize_rewrite_sum_if_to_count_if` rewrites `sum(if(cond, 0, 1))` into `countIf(not(cond))`,
-- which is wrong for a condition that can be NULL: `if` sends a NULL condition down the else
-- branch, while `not(NULL)` is NULL and `countIf` skips the row. The guard has to see through the
-- `LowCardinality` wrapper - an ordinary comparison over `LowCardinality(Nullable(String))`
-- produces `LowCardinality(Nullable(UInt8))`.

SET optimize_rewrite_sum_if_to_count_if = 1, optimize_rewrite_aggregate_function_with_if = 0;

DROP TABLE IF EXISTS t_sum_if_lc_nullable;
CREATE TABLE t_sum_if_lc_nullable (s LowCardinality(Nullable(String))) ENGINE = Memory;
INSERT INTO t_sum_if_lc_nullable VALUES ('y'), ('n'), (NULL);

SELECT toTypeName(s = 'y') FROM t_sum_if_lc_nullable LIMIT 1;

SELECT sum(if(s = 'y', 0, 1)) FROM t_sum_if_lc_nullable;
SELECT sum(if(s = 'y', 0, 1)) FROM t_sum_if_lc_nullable SETTINGS optimize_rewrite_sum_if_to_count_if = 0;

SELECT sum(if(s = 'y', 0, 123)) FROM t_sum_if_lc_nullable;
SELECT sum(if(s = 'y', 0, 123)) FROM t_sum_if_lc_nullable SETTINGS optimize_rewrite_sum_if_to_count_if = 0;

-- A plain Nullable condition was already refused, and stays refused.
SELECT sum(if(nullIf(s, 'zzz') = 'y', 0, 1)) FROM t_sum_if_lc_nullable;

-- The rewrite still fires for a condition that cannot be NULL.
SELECT 'not nullable';
DROP TABLE IF EXISTS t_sum_if_plain;
CREATE TABLE t_sum_if_plain (s LowCardinality(String)) ENGINE = Memory;
INSERT INTO t_sum_if_plain VALUES ('y'), ('n'), ('n');
SELECT sum(if(s = 'y', 0, 1)) FROM t_sum_if_plain;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT sum(if(s = 'y', 0, 1)) FROM t_sum_if_plain) WHERE explain LIKE '%countIf%';

DROP TABLE t_sum_if_lc_nullable;
DROP TABLE t_sum_if_plain;

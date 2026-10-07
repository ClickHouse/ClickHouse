-- Tags: shard

-- `buildQueryTreeForShard` keys its `GLOBAL IN` temporary table dedup map on an alias-blind tree
-- hash, so that repeated occurrences of one right-hand side share a single temporary table even
-- though `createUniqueAliasesIfNecessary` numbered their `__tableN` aliases differently.
--
-- Aliases are the only textual difference between the two right-hand sides below, so an alias-blind
-- key must still keep them apart: they select opposite sides of the same self-join and their sets
-- are different. It works because `getTreeHash` hashes a column's source as a back-reference once
-- it has been seen, so the pattern of repeated sources - which side of the join each column binds
-- to - is part of the hash regardless of aliases. Sharing therefore happens only between right-hand
-- sides whose bindings are isomorphic, which is exactly when the two sets are equal.
--
-- Every arm pairs the `GLOBAL IN` witness with a local `IN` control: `GLOBAL` changes only where
-- the set is built, never the result, so the two must agree.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_05136;

CREATE TABLE t_05136 (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05136 VALUES (1), (2);

-- `a.x + 1 = b.x` matches the single pair (1, 2), so `SELECT a.x` is {1} and `SELECT b.x` is {2}.

SELECT 'IN, opposite sides';
SELECT 2 IN (SELECT a.x FROM t_05136 AS a JOIN t_05136 AS b ON a.x + 1 = b.x) AS left_side,
       2 IN (SELECT b.x FROM t_05136 AS a JOIN t_05136 AS b ON a.x + 1 = b.x) AS right_side;

SELECT 'GLOBAL IN, opposite sides';
SELECT 2 GLOBAL IN (SELECT a.x FROM t_05136 AS a JOIN t_05136 AS b ON a.x + 1 = b.x) AS left_side,
       2 GLOBAL IN (SELECT b.x FROM t_05136 AS a JOIN t_05136 AS b ON a.x + 1 = b.x) AS right_side
FROM remote('127.0.0.1', system, one);

-- The right-hand side that occurs first must not win: check the reversed order too.
SELECT 'GLOBAL IN, opposite sides reversed';
SELECT 2 GLOBAL IN (SELECT b.x FROM t_05136 AS a JOIN t_05136 AS b ON a.x + 1 = b.x) AS right_side,
       2 GLOBAL IN (SELECT a.x FROM t_05136 AS a JOIN t_05136 AS b ON a.x + 1 = b.x) AS left_side
FROM remote('127.0.0.1', system, one);

-- Three occurrences: two of them are genuinely the same right-hand side and may share a temporary
-- table, the third one must not join them.
SELECT 'GLOBAL IN, repeated and opposite';
SELECT 2 GLOBAL IN (SELECT b.x FROM t_05136 AS a JOIN t_05136 AS b ON a.x + 1 = b.x) AS right_side,
       1 GLOBAL IN (SELECT b.x FROM t_05136 AS a JOIN t_05136 AS b ON a.x + 1 = b.x) AS right_side_other_value,
       1 GLOBAL IN (SELECT a.x FROM t_05136 AS a JOIN t_05136 AS b ON a.x + 1 = b.x) AS left_side
FROM remote('127.0.0.1', system, one);

-- `enable_add_distinct_to_in_subqueries` rewrites the right-hand sides before they are executed;
-- the rewrite must not make the two sides interchangeable either.
SELECT 'GLOBAL IN, opposite sides, DISTINCT rewrite';
SELECT 2 GLOBAL IN (SELECT a.x FROM t_05136 AS a JOIN t_05136 AS b ON a.x + 1 = b.x) AS left_side,
       2 GLOBAL IN (SELECT b.x FROM t_05136 AS a JOIN t_05136 AS b ON a.x + 1 = b.x) AS right_side
FROM remote('127.0.0.1', system, one)
SETTINGS enable_add_distinct_to_in_subqueries = 1;

DROP TABLE t_05136;

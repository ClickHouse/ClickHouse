-- Tags: distributed

-- An Alias hands out a storage snapshot of the table it points at, which the query neither owns nor
-- share-locks. Reading one inside a JOIN drives the join-reordering pass over that snapshot, and the
-- pass dereferences the storage behind it to build the hash-table cache key, so the snapshot has to
-- keep its referent alive. The Alias-of-Alias arm additionally covers a chain, where the snapshot
-- must stay bound to the innermost referent rather than to the intermediate Alias.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS outer_alias_05213;
DROP TABLE IF EXISTS inner_alias_05213;
DROP TABLE IF EXISTS dist_alias_05213;
DROP TABLE IF EXISTS dist_05213;
DROP TABLE IF EXISTS base_05213;
DROP TABLE IF EXISTS right_05213;

CREATE TABLE base_05213 (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE right_05213 (k UInt32, w String) ENGINE = MergeTree ORDER BY k;

-- CREATE rejects a target that already resolves to an Alias, so the chain is built target-last:
-- while inner_alias_05213 does not exist yet there is nothing to reject.
CREATE TABLE outer_alias_05213 ENGINE = Alias(currentDatabase(), inner_alias_05213);
CREATE TABLE inner_alias_05213 ENGINE = Alias(currentDatabase(), base_05213);

CREATE TABLE dist_05213 AS base_05213
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), base_05213);
CREATE TABLE dist_alias_05213 ENGINE = Alias(currentDatabase(), dist_05213);

INSERT INTO base_05213 VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO right_05213 VALUES (1, 'A'), (2, 'B'), (4, 'D');

SELECT 'alias_chain_join';
SELECT l.k, l.v, r.w FROM outer_alias_05213 AS l JOIN right_05213 AS r ON l.k = r.k ORDER BY ALL;

-- An Alias of a Distributed table is the shape that reached the freed storage: a Distributed table
-- in the query makes the planner build a throwaway analysis plan, which optimizes joins as well.
SELECT 'distributed_alias_join';
SELECT l.k, l.v, r.w FROM dist_alias_05213 AS l JOIN right_05213 AS r ON l.k = r.k ORDER BY ALL;

SELECT 'distributed_alias_join_explain_ok';
SELECT count() > 0 FROM (EXPLAIN SELECT l.k, r.w FROM dist_alias_05213 AS l JOIN right_05213 AS r ON l.k = r.k);

DROP TABLE outer_alias_05213;
DROP TABLE inner_alias_05213;
DROP TABLE dist_alias_05213;
DROP TABLE dist_05213;
DROP TABLE base_05213;
DROP TABLE right_05213;

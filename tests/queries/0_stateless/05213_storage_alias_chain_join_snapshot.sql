-- Tags: distributed
-- Random settings limits: query_plan_optimize_join_order_limit=(1, None)

-- An Alias hands out a storage snapshot of the table it points at, while the table expression that
-- receives that snapshot owns and share-locks the Alias rather than the target, so the snapshot is the
-- only thing that can keep the target alive. Collecting filters through an analysis plan is what
-- carries such a snapshot into the join-reordering pass: every table expression of the query is
-- replaced by a `StorageDummy` that republishes the original snapshot, and the pass dereferences the
-- storage behind it to build a hash-table cache key. That is the chain from the report, and driving it
-- plus the precondition assert on the snapshot's ownership token is what these queries check. Freeing
-- the target under a live plan needs a race with the background drop worker, so the use-after-free
-- itself is asserted by the unit test rather than here.
-- The Alias-of-Alias arm additionally covers a chain, where the token must stay bound to the innermost
-- referent rather than to the intermediate Alias.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS outer_alias_05213;
DROP TABLE IF EXISTS inner_alias_05213;
DROP TABLE IF EXISTS dist_alias_05213;
DROP TABLE IF EXISTS dist_05213;
DROP VIEW IF EXISTS view_right_05213;
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

-- Filter collection is decided per table expression, and an Alias is neither a Distributed nor a View
-- to that decision even when its target is, so the query needs a second table expression that asks for
-- it. Once any one does, the whole query is analysed through dummy tables, the Alias included.
CREATE VIEW view_right_05213 AS SELECT k, w FROM right_05213;

INSERT INTO base_05213 VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO right_05213 VALUES (1, 'A'), (2, 'B'), (4, 'D');

SELECT 'alias_chain_join';
SELECT l.k, l.v, r.w FROM outer_alias_05213 AS l JOIN right_05213 AS r ON l.k = r.k ORDER BY ALL;

-- An Alias of a Distributed table is the shape that reached the freed storage, so this is the arm that
-- carries an Alias-produced snapshot of a Distributed into the join-reordering pass.
SELECT 'distributed_alias_join';
SELECT l.k, l.v, r.w FROM dist_alias_05213 AS l JOIN view_right_05213 AS r ON l.k = r.k ORDER BY ALL;

SELECT 'distributed_alias_join_explain_ok';
SELECT count() > 0 FROM (EXPLAIN SELECT l.k, r.w FROM dist_alias_05213 AS l JOIN view_right_05213 AS r ON l.k = r.k);

DROP TABLE outer_alias_05213;
DROP TABLE inner_alias_05213;
DROP TABLE dist_alias_05213;
DROP TABLE dist_05213;
DROP VIEW view_right_05213;
DROP TABLE base_05213;
DROP TABLE right_05213;

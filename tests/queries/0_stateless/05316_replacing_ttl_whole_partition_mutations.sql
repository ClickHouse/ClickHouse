-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: the change is not ported to SharedMergeTree yet.

-- A mutation rewrites one part at a time, so for a ReplacingMergeTree whose row TTL needs a merge of the whole
-- partition (#122528) it must not delete rows by that TTL: the older version of a key in another part would become
-- visible again. `SYSTEM STOP TTL MERGES` does not stop mutations, so each case stops TTL merges and checks the
-- winner right after the mutation; then a merge of the whole partition deletes the key.

SET optimize_on_insert = 0;
SET mutations_sync = 2;

-- 1. MATERIALIZE TTL.
DROP TABLE IF EXISTS t_materialize;
CREATE TABLE t_materialize (k UInt64, v UInt64, d UInt8, exp DateTime)
ENGINE = ReplacingMergeTree(v) ORDER BY k
TTL exp DELETE WHERE d = 1
SETTINGS merge_selector_algorithm = 'Manual', materialize_ttl_recalculate_only = 0;

SYSTEM STOP TTL MERGES t_materialize;
INSERT INTO t_materialize VALUES (1, 1, 0, now() + INTERVAL 1 YEAR);
INSERT INTO t_materialize VALUES (1, 2, 1, now() - INTERVAL 1 DAY);
ALTER TABLE t_materialize MATERIALIZE TTL;
SELECT 'materialize winner', v, d FROM t_materialize FINAL;
SYSTEM START TTL MERGES t_materialize;
OPTIMIZE TABLE t_materialize FINAL;
SELECT 'materialize after optimize', count() FROM t_materialize FINAL;
DROP TABLE t_materialize;

-- 2. An UPDATE of the TTL column expires the newest version.
DROP TABLE IF EXISTS t_update;
CREATE TABLE t_update (k UInt64, v UInt64, d UInt8, exp DateTime)
ENGINE = ReplacingMergeTree(v) ORDER BY k
TTL exp DELETE WHERE d = 1
SETTINGS merge_selector_algorithm = 'Manual';

SYSTEM STOP TTL MERGES t_update;
INSERT INTO t_update VALUES (1, 1, 0, now() + INTERVAL 1 YEAR);
INSERT INTO t_update VALUES (1, 2, 1, now() + INTERVAL 1 YEAR);
ALTER TABLE t_update UPDATE exp = now() - INTERVAL 1 DAY WHERE v = 2;
SELECT 'update winner', v, d FROM t_update FINAL;
DROP TABLE t_update;

-- 3. MODIFY TTL materializes the new TTL on the existing parts.
DROP TABLE IF EXISTS t_modify;
CREATE TABLE t_modify (k UInt64, v UInt64, d UInt8, exp DateTime)
ENGINE = ReplacingMergeTree(v) ORDER BY k
SETTINGS merge_selector_algorithm = 'Manual';

SYSTEM STOP TTL MERGES t_modify;
INSERT INTO t_modify VALUES (1, 1, 0, now() - INTERVAL 1 DAY);
INSERT INTO t_modify VALUES (1, 2, 1, now() - INTERVAL 1 DAY);
ALTER TABLE t_modify MODIFY TTL exp DELETE WHERE d = 1;
SELECT 'modify winner', v, d FROM t_modify FINAL;
DROP TABLE t_modify;

-- 4. With `ttl_only_drop_parts`, any mutation used to replace a part whose rows had all expired with an empty part.
DROP TABLE IF EXISTS t_only_drop;
CREATE TABLE t_only_drop (k UInt64, v UInt64, p String, exp DateTime)
ENGINE = ReplacingMergeTree(v) ORDER BY k
TTL exp
SETTINGS merge_selector_algorithm = 'Manual', ttl_only_drop_parts = 1, materialize_ttl_recalculate_only = 0;

SYSTEM STOP TTL MERGES t_only_drop;
INSERT INTO t_only_drop VALUES (1, 1, 'a', now() + INTERVAL 1 YEAR);
INSERT INTO t_only_drop VALUES (1, 2, 'b', now() - INTERVAL 1 DAY);
ALTER TABLE t_only_drop UPDATE p = concat(p, '!') WHERE 1;
SELECT 'only drop update winner', v, p FROM t_only_drop FINAL;
ALTER TABLE t_only_drop MATERIALIZE TTL;
SELECT 'only drop materialize winner', v, p FROM t_only_drop FINAL;
SYSTEM START TTL MERGES t_only_drop;
OPTIMIZE TABLE t_only_drop FINAL;
SELECT 'only drop after optimize', count() FROM t_only_drop FINAL;
DROP TABLE t_only_drop;

-- 5. Control: a plain MergeTree still drops the expired part in a mutation.
DROP TABLE IF EXISTS t_only_drop_plain;
CREATE TABLE t_only_drop_plain (k UInt64, p String, exp DateTime)
ENGINE = MergeTree ORDER BY k
TTL exp
SETTINGS merge_selector_algorithm = 'Manual', ttl_only_drop_parts = 1, materialize_ttl_recalculate_only = 0;

SYSTEM STOP TTL MERGES t_only_drop_plain;
INSERT INTO t_only_drop_plain VALUES (1, 'a', now() + INTERVAL 1 YEAR);
INSERT INTO t_only_drop_plain VALUES (2, 'b', now() - INTERVAL 1 DAY);
ALTER TABLE t_only_drop_plain UPDATE p = concat(p, '!') WHERE 1;
SELECT 'plain only drop', k, p FROM t_only_drop_plain ORDER BY k;
DROP TABLE t_only_drop_plain;

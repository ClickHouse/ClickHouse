-- Regression test for use-after-free in HashJoin::addBlockToJoin with disjunctive ON clause.
--
-- When a JOIN has multiple ON clauses (A OR B), each clause has its own hash map.
-- If a right-side block has no new keys for maps[0] (!is_inserted), the code used to call
-- data->columns.pop_back() inside the onexpr_idx loop, leaving stored_columns as a dangling
-- pointer for subsequent iterations. Rows inserted into maps[1..N] in later iterations then
-- carried RowRefs pointing to freed memory, causing a LOGICAL_ERROR during probe.
--
-- Two separate INSERTs into a Memory table create two separate blocks, ensuring that the
-- second block (k1=1 duplicate) triggers the !is_inserted path for maps[0].

DROP TABLE IF EXISTS t_join_or_left;
DROP TABLE IF EXISTS t_join_or_right;

CREATE TABLE t_join_or_left  (k1 UInt64, k2 UInt64, v String) ENGINE = Memory;
CREATE TABLE t_join_or_right (k1 UInt64, k2 UInt64, v String) ENGINE = Memory;

INSERT INTO t_join_or_left VALUES (1, 99, 'l1'), (99, 20, 'l2');

-- Block 1: k1=1 is new in maps[0], k2=10 is new in maps[1]
INSERT INTO t_join_or_right VALUES (1, 10, 'r1');
-- Block 2: k1=1 is a duplicate in maps[0] (triggers !is_inserted for onexpr_idx=0),
--          but k2=20 is new in maps[1]. With the bug, the block is popped after onexpr_idx=0,
--          creating a dangling RowRef for the maps[1] entry.
INSERT INTO t_join_or_right VALUES (1, 20, 'r2');

SELECT t_join_or_left.k1, t_join_or_left.k2, t_join_or_right.k1, t_join_or_right.v
FROM t_join_or_left
ANY INNER JOIN t_join_or_right ON (t_join_or_left.k1 = t_join_or_right.k1 OR t_join_or_left.k2 = t_join_or_right.k2)
ORDER BY t_join_or_left.k1;

DROP TABLE t_join_or_left;
DROP TABLE t_join_or_right;

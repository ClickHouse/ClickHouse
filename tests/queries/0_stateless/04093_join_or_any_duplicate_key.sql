-- Regression tests for use-after-free in `HashJoin::addBlockToJoin` with disjunctive ON clause.
-- Both tests are reliable reproducers only under ASan.

-- Test 1: duplicate key in maps[0] triggers !is_inserted mid-loop.
-- The second right block (k1=1 duplicate) used to cause data->columns.pop_back() inside the
-- onexpr_idx loop, leaving stored_columns dangling for later clause iterations.

SELECT 'test 1: duplicate key';

DROP TABLE IF EXISTS t_join_or_left;
DROP TABLE IF EXISTS t_join_or_right;

CREATE TABLE t_join_or_left  (k1 UInt64, k2 UInt64, v String) ENGINE = Memory;
CREATE TABLE t_join_or_right (k1 UInt64, k2 UInt64, v String) ENGINE = Memory;

INSERT INTO t_join_or_left VALUES (1, 99, 'l1'), (99, 20, 'l2');

-- Block 1: k1=1 is new in maps[0], k2=10 is new in maps[1].
INSERT INTO t_join_or_right VALUES (1, 10, 'r1');
-- Block 2: k1=1 is a duplicate in maps[0] (!is_inserted for onexpr_idx=0),
--          but k2=20 is new in maps[1].
INSERT INTO t_join_or_right VALUES (1, 20, 'r2');

SELECT t_join_or_left.k1, t_join_or_left.k2, t_join_or_right.k1, t_join_or_right.v
FROM t_join_or_left
ANY INNER JOIN t_join_or_right ON (t_join_or_left.k1 = t_join_or_right.k1 OR t_join_or_left.k2 = t_join_or_right.k2)
ORDER BY t_join_or_left.k1;

DROP TABLE t_join_or_left;
DROP TABLE t_join_or_right;

-- Test 2: has_right_not_joined=true with inserted_count=0 (RIGHT/FULL join with ON condition mask).
-- Block 2 row (k1=99, k2=10, flag=0): clause 0 condition flag=1 fails → is_inserted=false,
-- has_right_not_joined=true → nullmap added referencing stored_columns. Clause 1 key k2=10
-- is a duplicate → is_inserted=false. inserted_count=0 but nullmaps grew, so pop_back must
-- be skipped or the nullmap holds a dangling pointer during NotJoinedHash iteration.

SELECT 'test 2: has_right_not_joined with inserted_count=0';

DROP TABLE IF EXISTS t_right_nj_left;
DROP TABLE IF EXISTS t_right_nj_right;

CREATE TABLE t_right_nj_left  (k1 UInt64, k2 UInt64) ENGINE = Memory;
CREATE TABLE t_right_nj_right (k1 UInt64, k2 UInt64, flag UInt8) ENGINE = Memory;

INSERT INTO t_right_nj_left VALUES (1, 10);

-- Block 1: k1=1 (flag=1 passes clause 0) → maps[0][k1=1]; k2=10 → maps[1][k2=10].
INSERT INTO t_right_nj_right VALUES (1, 10, 1);
-- Block 2: k1=99 new but flag=0 fails clause 0 → is_inserted=false, has_right_not_joined=true.
--          k2=10 duplicate in clause 1 → is_inserted=false. inserted_count=0, nullmaps grew.
INSERT INTO t_right_nj_right VALUES (99, 10, 0);

SELECT r.k1, r.k2, r.flag, l.k1 AS l_k1
FROM t_right_nj_left AS l
ANY RIGHT JOIN t_right_nj_right AS r
    ON (l.k1 = r.k1 AND r.flag = 1) OR (l.k2 = r.k2)
ORDER BY r.k1
SETTINGS join_algorithm = 'hash';

DROP TABLE t_right_nj_left;
DROP TABLE t_right_nj_right;

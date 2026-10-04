-- Tags: no-parallel
-- ^ row policies are global

SET enable_analyzer = 1;
SET allow_experimental_lookup_index = 1;

DROP TABLE IF EXISTS t_lookup_rp_dim SYNC;
DROP TABLE IF EXISTS t_lookup_rp_fact SYNC;
DROP ROW POLICY IF EXISTS pol_04244_set ON t_lookup_rp_dim;

CREATE TABLE t_lookup_rp_dim
(
    id UInt64,
    val String,
    LOOKUP INDEX idx_set (id) TYPE table_set,
    LOOKUP INDEX idx_join (id) TYPE table_join
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE t_lookup_rp_fact
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_lookup_rp_dim VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t_lookup_rp_fact VALUES (1, 'x'), (2, 'y'), (3, 'z');

-- Baseline: no row policy. Both lookup paths should return the full intersection.
-- `t_lookup_rp_dim` has an extra `val` column, so use the key-only subquery form
-- (the trivial-projection branch of the lookup-set fast path). The direct join
-- exercises the `table_join` lookup fast path.

SELECT 'set: no row policy';
SELECT id FROM t_lookup_rp_fact WHERE id IN (SELECT id FROM t_lookup_rp_dim) ORDER BY id;
SELECT 'join: no row policy';
SELECT f.id, d.val FROM t_lookup_rp_fact f JOIN t_lookup_rp_dim d USING (id)
ORDER BY f.id
SETTINGS join_algorithm = 'direct';

-- Restrict the right table to id = 1 only.
-- Both the table_set fast path (CollectSets) and the table_join lookup fast path
-- (PlannerJoins) must fall back to the regular path so the policy is honored.
-- The set case still uses the subquery form (`IN (SELECT id FROM ...)`); the join
-- case falls back to hash since the direct path on plain MergeTree right tables
-- is gated on the absence of row policies.

CREATE ROW POLICY pol_04244_set ON t_lookup_rp_dim USING id = 1 TO ALL;

SELECT 'set: with row policy id = 1';
SELECT id FROM t_lookup_rp_fact WHERE id IN (SELECT id FROM t_lookup_rp_dim) ORDER BY id;
SELECT 'join: with row policy id = 1';
SELECT f.id, d.val FROM t_lookup_rp_fact f JOIN t_lookup_rp_dim d USING (id)
ORDER BY f.id;

DROP ROW POLICY pol_04244_set ON t_lookup_rp_dim;
DROP TABLE t_lookup_rp_fact SYNC;
DROP TABLE t_lookup_rp_dim SYNC;

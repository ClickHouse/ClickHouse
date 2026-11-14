DROP TABLE IF EXISTS t_lwu_pending_mutation;
DROP TABLE IF EXISTS t_lwu_pending_mutation_overwrites;

-- A mutation that is still pending is applied on the fly by a reader step of its own, and a lightweight
-- `UPDATE` issued after it is applied only at the boundary of that step. A `DEFAULT` column that reads
-- the updated column and is computed by that step must see the updated value.

CREATE TABLE t_lwu_pending_mutation (x UInt32) ENGINE = MergeTree ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_pending_mutation SELECT number FROM numbers(6);

ALTER TABLE t_lwu_pending_mutation ADD COLUMN a UInt32 DEFAULT 0, ADD COLUMN w UInt32 DEFAULT 7;
ALTER TABLE t_lwu_pending_mutation ADD COLUMN z UInt32 DEFAULT a + 1000;

SYSTEM STOP MERGES t_lwu_pending_mutation;

-- Stays pending.
ALTER TABLE t_lwu_pending_mutation UPDATE w = 8 WHERE 1 SETTINGS mutations_sync = 0;
-- A later patch.
UPDATE t_lwu_pending_mutation SET a = x + 5 WHERE x % 2 = 0;

SELECT 'pending mutation not applied';
SELECT x, a, z, w FROM t_lwu_pending_mutation ORDER BY x SETTINGS apply_mutations_on_fly = 0;

SELECT 'pending mutation applied on the fly';
SELECT x, a, z, w FROM t_lwu_pending_mutation ORDER BY x SETTINGS apply_mutations_on_fly = 1;

SELECT 'pending mutation applied on the fly, dependents only';
SELECT z, w FROM t_lwu_pending_mutation ORDER BY z, w SETTINGS apply_mutations_on_fly = 1;

SYSTEM START MERGES t_lwu_pending_mutation;
ALTER TABLE t_lwu_pending_mutation UPDATE w = w WHERE 0 SETTINGS mutations_sync = 2;

SELECT 'mutation materialized';
SELECT x, a, z, w FROM t_lwu_pending_mutation ORDER BY x;

-- The pending mutation overwrites the `DEFAULT` column itself: the value the mutation gives stays, as it
-- does once the mutation has materialized the column in the part. The mutation predates the lightweight
-- `UPDATE`, so it materializes `z` from `a = 0`: `3` where it matches and `1000` elsewhere.
-- The result after materialization is not checked here: another mutation issued to wait for it may be
-- squashed together with it into one mutation with a version above the patch, which then sees the patched `a`.

CREATE TABLE t_lwu_pending_mutation_overwrites (x UInt32) ENGINE = MergeTree ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_pending_mutation_overwrites SELECT number FROM numbers(6);

ALTER TABLE t_lwu_pending_mutation_overwrites ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_pending_mutation_overwrites ADD COLUMN z UInt32 DEFAULT a + 1000;

SYSTEM STOP MERGES t_lwu_pending_mutation_overwrites;

ALTER TABLE t_lwu_pending_mutation_overwrites UPDATE z = 3 WHERE x % 3 = 0 SETTINGS mutations_sync = 0;
UPDATE t_lwu_pending_mutation_overwrites SET a = x + 5 WHERE x % 2 = 0;

SELECT 'overwriting mutation applied on the fly';
SELECT x, a, z FROM t_lwu_pending_mutation_overwrites ORDER BY x SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_lwu_pending_mutation;
DROP TABLE t_lwu_pending_mutation_overwrites;

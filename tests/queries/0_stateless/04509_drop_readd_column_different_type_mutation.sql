-- Reproduce Fatal: DROP COLUMN + re-ADD with a different type causes type mismatch
-- during mutation execution when mutations are squashed.
--
-- When a part needs to be mutated through DROP COLUMN (Nullable(Int32)) and the column
-- was later re-added as UInt64, the mutation interpreter builds an ifNull expression
-- using the wrong type, causing a logical error.

SET mutations_sync = 0;
SET alter_sync = 0;

DROP TABLE IF EXISTS t_drop_readd_type;

CREATE TABLE t_drop_readd_type
(
    id UInt64,
    value UInt64
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_drop_readd_type VALUES (1, 100), (2, 200), (3, 300);

-- Add a Nullable(Int32) column.
ALTER TABLE t_drop_readd_type ADD COLUMN extra Nullable(Int32);

-- Insert data with the Nullable(Int32) column.
INSERT INTO t_drop_readd_type VALUES (4, 400, 42), (5, 500, 55), (6, 600, NULL);

-- Stop merges so mutations accumulate but don't execute on parts.
SYSTEM STOP MERGES t_drop_readd_type;

-- Queue mutation 1: DELETE (forces part to be rewritten).
ALTER TABLE t_drop_readd_type DELETE WHERE id = 999;

-- Queue mutation 2: DROP COLUMN extra (the Nullable(Int32) column).
ALTER TABLE t_drop_readd_type DROP COLUMN extra;

-- Metadata-only change: ADD COLUMN extra with a DIFFERENT type (UInt64), no DEFAULT.
ALTER TABLE t_drop_readd_type ADD COLUMN extra UInt64;

-- Queue mutation 3: another DELETE to ensure squash covers all mutations.
ALTER TABLE t_drop_readd_type DELETE WHERE id = 998;

-- Let all mutations run at once on the original parts.
SYSTEM START MERGES t_drop_readd_type;

-- Wait for all mutations to complete.
SET mutations_sync = 2;
ALTER TABLE t_drop_readd_type DELETE WHERE id = 997;

-- Verify data is correct.
SELECT id, value, extra FROM t_drop_readd_type ORDER BY id;

DROP TABLE t_drop_readd_type;

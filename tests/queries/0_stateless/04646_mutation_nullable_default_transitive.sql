-- When a mutation reads a column whose part data is `Nullable` but whose metadata type is not,
-- the `NULL`s are replaced by the column default, so the mutation must also read the columns that
-- default expression needs. Those columns may themselves need the same conversion, so the set of
-- required columns has to be walked until it stops growing, not only once.

DROP TABLE IF EXISTS t_nullable_default_chain;
CREATE TABLE t_nullable_default_chain
(
    a Nullable(UInt64),
    b Nullable(UInt64),
    c Nullable(UInt64),
    v UInt64,
    k UInt64
) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_nullable_default_chain VALUES (NULL, NULL, NULL, 0, 1), (10, 20, 30, 0, 2);

SYSTEM STOP MERGES t_nullable_default_chain;

-- `b` defaults from `c`, and `c` in turn defaults from `a`, so resolving `b` for a part that still
-- holds `NULL`s has to pull in `c` and then `a`.
ALTER TABLE t_nullable_default_chain MODIFY COLUMN a UInt64 DEFAULT 1 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_nullable_default_chain MODIFY COLUMN c UInt64 DEFAULT a + 1 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_nullable_default_chain MODIFY COLUMN b UInt64 DEFAULT c + 1 SETTINGS mutations_sync = 0, alter_sync = 0;

-- The unconverted part reads through the whole chain.
SELECT a, b, c FROM t_nullable_default_chain ORDER BY k;

-- A mutation reading `b` must resolve the chain as well.
SYSTEM START MERGES t_nullable_default_chain;
ALTER TABLE t_nullable_default_chain UPDATE v = b WHERE k = 1 SETTINGS mutations_sync = 2;

SELECT a, b, c, v FROM t_nullable_default_chain ORDER BY k;

DROP TABLE t_nullable_default_chain;

-- `CLEAR COLUMN` must recompute exactly the `MATERIALIZED` columns that the cleared column makes
-- stale - directly or transitively - and leave every other one alone, so that the metadata-only
-- semantics of `ALTER TABLE ... MODIFY COLUMN ... MATERIALIZED` (existing parts keep their values)
-- are preserved.

SET mutations_sync = 2;

SELECT '-- only the dependency closure is recomputed';

DROP TABLE IF EXISTS t_clear_closure;

CREATE TABLE t_clear_closure
(
    id UInt64,
    c UInt64,
    unrelated UInt64 MATERIALIZED 100,
    direct UInt64 MATERIALIZED c + 1,
    transitive UInt64 MATERIALIZED direct * 10
)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_clear_closure (id, c) VALUES (1, 5);

-- Change the expressions of all three MATERIALIZED columns. These are metadata-only ALTERs,
-- so the part still stores the old values.
ALTER TABLE t_clear_closure MODIFY COLUMN unrelated UInt64 MATERIALIZED 200;
ALTER TABLE t_clear_closure MODIFY COLUMN direct UInt64 MATERIALIZED c + 2;
ALTER TABLE t_clear_closure MODIFY COLUMN transitive UInt64 MATERIALIZED direct * 100;

SELECT id, c, unrelated, direct, transitive FROM t_clear_closure ORDER BY id;

ALTER TABLE t_clear_closure CLEAR COLUMN c;

-- `c` is now 0, so `direct` becomes 2 and `transitive` becomes 200 (recomputed with the new
-- expressions), while `unrelated` keeps the value stored at INSERT time (100, not 200).
SELECT id, c, unrelated, direct, transitive FROM t_clear_closure ORDER BY id;

DROP TABLE t_clear_closure;

SELECT '-- a MATERIALIZED column made stale through another one cannot be silently kept';

DROP TABLE IF EXISTS t_clear_transitive_ephemeral;

CREATE TABLE t_clear_transitive_ephemeral
(
    id UInt64,
    c UInt64,
    e UInt64 EPHEMERAL 7,
    m1 UInt64 MATERIALIZED c + 1,
    m2 UInt64 MATERIALIZED m1 + e
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_clear_transitive_ephemeral (id, c, e) VALUES (1, 5, 7);

-- `m2` reads `m1`, which reads the cleared `c`, so `m2` would have to be recomputed too - but it
-- also reads the EPHEMERAL `e`, which cannot be read back from the part. Reject the ALTER instead
-- of committing an `m2` that no longer matches its declared expression.
ALTER TABLE t_clear_transitive_ephemeral CLEAR COLUMN c; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_clear_transitive_ephemeral';
SELECT id, c, m1, m2 FROM t_clear_transitive_ephemeral ORDER BY id;

-- Clearing a column outside the closure of the EPHEMERAL-dependent column is still allowed.
ALTER TABLE t_clear_transitive_ephemeral ADD COLUMN other UInt64;
ALTER TABLE t_clear_transitive_ephemeral CLEAR COLUMN other;
SELECT id, c, m1, m2 FROM t_clear_transitive_ephemeral ORDER BY id;

DROP TABLE t_clear_transitive_ephemeral;

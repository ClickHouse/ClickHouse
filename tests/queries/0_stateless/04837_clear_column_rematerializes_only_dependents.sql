-- A `CLEAR COLUMN` that makes any `MATERIALIZED` column stale re-evaluates every `MATERIALIZED`
-- expression of the table, so a column whose expression was changed by a metadata-only
-- `ALTER TABLE ... MODIFY COLUMN ... MATERIALIZED` is refreshed as well. The dependency closure of
-- the cleared column still matters: it decides the order the columns are recomputed in, which
-- columns make the ALTER fail up front, and which columns are left alone because recomputing them
-- is not safe (a sorting or partition key input, or a column reading an `EPHEMERAL` column).

SET mutations_sync = 2;

SELECT '-- the dependency closure is recomputed in order, together with the other columns';

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

-- `c` is now 0, so `direct` becomes 2 and `transitive` becomes 200: `transitive` is evaluated
-- against the freshly recomputed `direct`, not against its pre-clear value. `unrelated` reads
-- nothing that the clear touches, but it is re-evaluated too and picks up its new expression.
SELECT id, c, unrelated, direct, transitive FROM t_clear_closure ORDER BY id;

DROP TABLE t_clear_closure;

SELECT '-- a key column outside the closure keeps its stored value';

DROP TABLE IF EXISTS t_clear_key_materialized;

-- `k` orders the part, so rewriting it in place could break the sort order. The clear does not
-- make it stale (it does not read `c`), so it is simply left alone instead of failing the ALTER.
CREATE TABLE t_clear_key_materialized
(
    id UInt64,
    c UInt64,
    src UInt64,
    k UInt64 MATERIALIZED src,
    direct UInt64 MATERIALIZED c + 1
)
ENGINE = MergeTree ORDER BY (k, id) SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_clear_key_materialized (id, c, src) VALUES (1, 5, 3);

ALTER TABLE t_clear_key_materialized MODIFY COLUMN k UInt64 MATERIALIZED src + 100;

ALTER TABLE t_clear_key_materialized CLEAR COLUMN c;

-- `direct` is recomputed from the cleared `c`, while `k` still holds the value written at INSERT.
SELECT id, c, src, k, direct FROM t_clear_key_materialized ORDER BY id;

DROP TABLE t_clear_key_materialized;

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

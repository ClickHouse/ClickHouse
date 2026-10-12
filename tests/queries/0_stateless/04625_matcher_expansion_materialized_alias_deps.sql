-- A `MATERIALIZED` column can change its effective expression through the body of a referenced
-- `ALIAS` column (including matcher re-expansion inside that body on an unrelated ALTER), and
-- rematerializing one `MATERIALIZED` column must also recalculate downstream `MATERIALIZED`
-- columns that depend on it. If the affected column is in the sort key, the ALTER is rejected
-- up front.

SET alter_sync = 2;
SET mutations_sync = 2;

-- 1. The effective expression change arrives through an `ALIAS` body: `m MATERIALIZED y` where
--    `y ALIAS greatest(*, 0)`. `ADD COLUMN b` changes the expansion of `*` inside `y`, so `m`
--    must be rematerialized even though its own AST (`y`) does not change.
SELECT '-- materialized via alias body: rematerialized on ADD COLUMN';
DROP TABLE IF EXISTS t_mat_alias;
CREATE TABLE t_mat_alias
(
    a UInt64,
    y UInt64 ALIAS greatest(*, 0),
    m UInt64 MATERIALIZED y
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_mat_alias SELECT number FROM numbers(3);

-- `y` was `greatest(a, 0)`; after adding `b` it becomes `greatest(a, b, 0)` = `a + 1000`.
ALTER TABLE t_mat_alias ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mat_alias' AND command ILIKE '%MATERIALIZE COLUMN%';

-- Existing parts follow the new expansion, and new inserts agree with them.
INSERT INTO t_mat_alias (a) VALUES (10);
SELECT a, m FROM t_mat_alias ORDER BY a;
SELECT count() FROM t_mat_alias WHERE m != greatest(a, b, 0);

-- An explicit edit of the referenced `ALIAS` body changes the effective expression of `m` too.
ALTER TABLE t_mat_alias MODIFY COLUMN y UInt64 ALIAS least(*, 100000);

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mat_alias' AND command ILIKE '%MATERIALIZE COLUMN%';
SELECT count() FROM t_mat_alias WHERE m != least(a, b, 100000);

DROP TABLE t_mat_alias;

-- 2. Rematerializing a `MATERIALIZED` column recalculates downstream `MATERIALIZED` columns
--    transitively: `m1` changes its matcher expansion, `m2` depends on `m1`, `m3` on `m2`.
SELECT '-- dependent materialized closure: recalculated transitively';
DROP TABLE IF EXISTS t_mat_chain;
CREATE TABLE t_mat_chain
(
    a UInt64,
    m1 UInt64 MATERIALIZED greatest(a, *),
    m2 UInt64 MATERIALIZED m1 + 1,
    m3 UInt64 MATERIALIZED m2 + 1
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_mat_chain SELECT number FROM numbers(3);

-- `m1` was `greatest(a, a)`; after adding `b` it becomes `greatest(a, a, b)` = `a + 1000`,
-- so old parts must get `m1 = a + 1000`, `m2 = a + 1001`, `m3 = a + 1002`.
ALTER TABLE t_mat_chain ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT a, m1, m2, m3 FROM t_mat_chain ORDER BY a;
SELECT count() FROM t_mat_chain WHERE m1 != a + 1000 OR m2 != a + 1001 OR m3 != a + 1002;

DROP TABLE t_mat_chain;

-- 3. An explicit `MATERIALIZE COLUMN` rewrites only the named column; the user controls
--    which columns to backfill and when. `MODIFY COLUMN ... MATERIALIZED` is metadata-only,
--    so old parts keep `m1 = a + 1`, `m2 = (a + 1) * 10`; `MATERIALIZE COLUMN m1` rewrites
--    `m1 = a + 2` and leaves `m2` untouched. Materializing both in one ALTER recalculates
--    `m2` from the already recalculated `m1`.
SELECT '-- explicit MATERIALIZE COLUMN: only the named column';
DROP TABLE IF EXISTS t_mat_explicit;
CREATE TABLE t_mat_explicit
(
    a UInt64,
    m1 UInt64 MATERIALIZED a + 1,
    m2 UInt64 MATERIALIZED m1 * 10
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_mat_explicit (a) SELECT number FROM numbers(3);
ALTER TABLE t_mat_explicit MODIFY COLUMN m1 UInt64 MATERIALIZED a + 2;
ALTER TABLE t_mat_explicit MATERIALIZE COLUMN m1;

SELECT a, m1, m2 FROM t_mat_explicit ORDER BY a;

ALTER TABLE t_mat_explicit MATERIALIZE COLUMN m1, MATERIALIZE COLUMN m2;

SELECT a, m1, m2 FROM t_mat_explicit ORDER BY a;

DROP TABLE t_mat_explicit;

-- 4. If the `MATERIALIZED` column whose expansion changes is in the sort key, the ALTER is
--    rejected up front and the metadata is not changed.
SELECT '-- sort-key materialized column: ALTER rejected up front';
DROP TABLE IF EXISTS t_mat_sort_key;
CREATE TABLE t_mat_sort_key
(
    a UInt64,
    m UInt64 MATERIALIZED greatest(a, *)
) ENGINE = MergeTree ORDER BY m;

INSERT INTO t_mat_sort_key (a) SELECT number FROM numbers(3);

ALTER TABLE t_mat_sort_key ADD COLUMN b UInt64 DEFAULT a + 1000; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT count() FROM system.columns
WHERE database = currentDatabase() AND table = 't_mat_sort_key' AND name = 'b';

DROP TABLE t_mat_sort_key;

-- 5. The same rejection applies when the sort-key column is a dependent of the changed one:
--    the dependent closure would have to rematerialize it.
DROP TABLE IF EXISTS t_mat_sort_key_dep;
CREATE TABLE t_mat_sort_key_dep
(
    a UInt64,
    m1 UInt64 MATERIALIZED greatest(a, *),
    m2 UInt64 MATERIALIZED m1 + 1
) ENGINE = MergeTree ORDER BY m2;

INSERT INTO t_mat_sort_key_dep (a) SELECT number FROM numbers(3);

ALTER TABLE t_mat_sort_key_dep ADD COLUMN b UInt64 DEFAULT a + 1000; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT count() FROM system.columns
WHERE database = currentDatabase() AND table = 't_mat_sort_key_dep' AND name = 'b';

DROP TABLE t_mat_sort_key_dep;

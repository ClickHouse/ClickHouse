-- A later `UPDATE a = 'x'` puts `a` in the on-fly chain's overwritten set, while an
-- earlier `UPDATE b = isNotNull(materialize(a))` step still reads `a` as a function
-- input. When the query reads `a` too (so the `UPDATE a` is not dropped), `a` must
-- still be converted to the post-`MODIFY` `LowCardinality(Nullable(String))` type the
-- `materialize(a)` action expects. Without the fix `materialize` aborts with
-- `LOGICAL_ERROR: Unexpected return type from materialize`.

DROP TABLE IF EXISTS t_read_overwritten_lc SYNC;

CREATE TABLE t_read_overwritten_lc
(
    id UInt64,
    a Nullable(String),
    b UInt8
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_read_overwritten_lc
SELECT number, if(number % 2 = 0, NULL, toString(number)), 0
FROM numbers(100);

SYSTEM STOP MERGES t_read_overwritten_lc;

ALTER TABLE t_read_overwritten_lc
    UPDATE b = isNotNull(materialize(a)) WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;

ALTER TABLE t_read_overwritten_lc
    UPDATE a = 'x' WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;

ALTER TABLE t_read_overwritten_lc
    MODIFY COLUMN a LowCardinality(Nullable(String))
    SETTINGS mutations_sync = 0, alter_sync = 0;

-- Reads both `b` and `a`: `UPDATE a` survives, putting `a` in the overwritten set,
-- but `materialize(a)` in the `UPDATE b` step still consumes `a`.
SELECT sum(b), any(a)
FROM t_read_overwritten_lc
SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;

SELECT a, b
FROM t_read_overwritten_lc
ORDER BY id
LIMIT 4
SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;

SELECT sum(b)
FROM t_read_overwritten_lc
WHERE a IS NOT NULL
SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;

DROP TABLE t_read_overwritten_lc SYNC;

-- The forced conversion must apply ONLY to on-fly mutation steps, not to trailing query
-- PREWHERE steps. Here `v` holds an unconvertible on-disk value `'x'`, `UPDATE v = '100'`
-- overwrites it before `MODIFY COLUMN v UInt64`, and a query `PREWHERE v > 50` consumes `v`.
-- The on-disk `'x'` must be discarded by the UPDATE, NOT pre-cast to `UInt64`: forcing the
-- query PREWHERE consumption to convert `v` would raise `CANNOT_PARSE_TEXT`.
DROP TABLE IF EXISTS t_overwrite_then_prewhere SYNC;

CREATE TABLE t_overwrite_then_prewhere
(
    id UInt64,
    v String,
    b UInt8
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_overwrite_then_prewhere SELECT number, 'x', number % 2 FROM numbers(10);

SYSTEM STOP MERGES t_overwrite_then_prewhere;

ALTER TABLE t_overwrite_then_prewhere
    UPDATE v = '100' WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;

ALTER TABLE t_overwrite_then_prewhere
    MODIFY COLUMN v UInt64
    SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT sum(b) FROM t_overwrite_then_prewhere PREWHERE v > 50
SETTINGS apply_mutations_on_fly = 1;

SELECT v, sum(b) FROM t_overwrite_then_prewhere GROUP BY v
SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_overwrite_then_prewhere SYNC;

-- Same chain as the first table, but the consumed/overwritten column is a PHYSICAL column
-- whose name literally contains a dot (`info.name`, not a Tuple subcolumn). The consumed-set
-- key must be the column's storage name (`info.name`), matching the skip set. Keying it by
-- the part before the first dot (`info`) would miss it and skip the conversion, reproducing
-- `LOGICAL_ERROR: Unexpected return type from materialize`.
DROP TABLE IF EXISTS t_dotted_physical SYNC;

CREATE TABLE t_dotted_physical
(
    id UInt64,
    `info.name` Nullable(String),
    b UInt8
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_dotted_physical
SELECT number, if(number % 2 = 0, NULL, toString(number)), 0
FROM numbers(100);

SYSTEM STOP MERGES t_dotted_physical;

ALTER TABLE t_dotted_physical
    UPDATE b = isNotNull(materialize(`info.name`)) WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;

ALTER TABLE t_dotted_physical
    UPDATE `info.name` = 'x' WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;

ALTER TABLE t_dotted_physical
    MODIFY COLUMN `info.name` LowCardinality(Nullable(String))
    SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT sum(b), any(`info.name`)
FROM t_dotted_physical
SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;

SELECT `info.name`, b
FROM t_dotted_physical
ORDER BY id
LIMIT 4
SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;

DROP TABLE t_dotted_physical SYNC;

-- The exclusion must be ordered per step, not chain-wide. Here the overwriting step
-- (`UPDATE v = toString(id + 100)`) comes BEFORE the consumer (`UPDATE b = materialize(v)`),
-- and the overwrite expression itself reads the old `v` (`if(cond, _CAST(expr), v)`). The
-- on-disk `'x'` is unconvertible to the post-`MODIFY` `UInt64`, so it must stay skipped: the
-- consumer reads the value the UPDATE produced, not the on-disk one. Forcing the conversion
-- because some step consumes `v` raises `CANNOT_PARSE_TEXT` on the discarded `'x'`.
DROP TABLE IF EXISTS t_overwrite_before_consume SYNC;

CREATE TABLE t_overwrite_before_consume
(
    id UInt64,
    v String,
    b UInt8
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_overwrite_before_consume SELECT number, 'x', 0 FROM numbers(10);

SYSTEM STOP MERGES t_overwrite_before_consume;

ALTER TABLE t_overwrite_before_consume
    UPDATE v = toString(id + 100) WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;

ALTER TABLE t_overwrite_before_consume
    UPDATE b = isNotNull(materialize(v)) WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;

ALTER TABLE t_overwrite_before_consume
    MODIFY COLUMN v UInt64
    SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT v, b
FROM t_overwrite_before_consume
ORDER BY id
LIMIT 2
SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;

DROP TABLE t_overwrite_before_consume SYNC;

-- A single step that BOTH overwrites and genuinely reads the column (`UPDATE a =
-- materialize(a)`): `a` is consumed by `materialize(a)` (a real read, needs the post-`MODIFY`
-- type) and again as the `if` keep-old fallback (synthetic, must stay skipped). The genuine
-- read must win, otherwise `materialize` runs on the on-disk type and aborts with
-- `LOGICAL_ERROR: Unexpected return type from materialize`.
DROP TABLE IF EXISTS t_self_read_overwrite SYNC;

CREATE TABLE t_self_read_overwrite
(
    id UInt64,
    a Nullable(String),
    b UInt8
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_self_read_overwrite
SELECT number, if(number % 2 = 0, NULL, toString(number)), 0
FROM numbers(100);

SYSTEM STOP MERGES t_self_read_overwrite;

ALTER TABLE t_self_read_overwrite
    UPDATE a = materialize(a) WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;

ALTER TABLE t_self_read_overwrite
    MODIFY COLUMN a LowCardinality(Nullable(String))
    SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT any(a), sum(b)
FROM t_self_read_overwrite
SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;

SELECT a
FROM t_self_read_overwrite
ORDER BY id
LIMIT 4
SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;

DROP TABLE t_self_read_overwrite SYNC;

-- The keep-old fallback IS read when the UPDATE condition is not constant-true. With a
-- partial `UPDATE v = 100 WHERE id > 5`, the lowered `if(id > 5, _CAST(100, UInt64), v)` is
-- declared UInt64, and for the unmatched rows `FunctionIf` returns the `v` fallback. That `v`
-- must arrive in the post-`MODIFY` UInt64 type, or the executor aborts with
-- `LOGICAL_ERROR: Unexpected return type from if. Expected UInt64. Got String`. The on-disk
-- values are convertible, so the result must match a synchronous UPDATE + MODIFY.
DROP TABLE IF EXISTS t_keep_old_nonconst_cond SYNC;

CREATE TABLE t_keep_old_nonconst_cond
(
    id UInt64,
    v String,
    b UInt8
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_keep_old_nonconst_cond SELECT number, toString(number * 10), number % 2 FROM numbers(10);

SYSTEM STOP MERGES t_keep_old_nonconst_cond;

ALTER TABLE t_keep_old_nonconst_cond
    UPDATE v = 100 WHERE id > 5
    SETTINGS mutations_sync = 0, alter_sync = 0;

ALTER TABLE t_keep_old_nonconst_cond
    MODIFY COLUMN v UInt64
    SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT v, b
FROM t_keep_old_nonconst_cond
ORDER BY id
LIMIT 8
SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_keep_old_nonconst_cond SYNC;

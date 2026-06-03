-- Regression test from `clickhouse-gh[bot]` review on PR #105847
-- (https://github.com/ClickHouse/ClickHouse/pull/105847#discussion_r3350248306
-- and https://github.com/ClickHouse/ClickHouse/pull/105847#discussion_r3350564104).
--
-- Two related scenarios for the on-fly chain when an `UPDATE b = f(a)` is
-- followed by an `UPDATE a = ...` and a pending `MODIFY COLUMN a`.
--
-- Variant 1 (`SELECT sum(b)`): `filterMutationCommands` drops `UPDATE a`
-- because the read does not need `a` directly. If the skip set used to gate
-- `performRequiredConversions` is built from the full `mutation_commands`
-- list it still excludes `a`, but the surviving `UPDATE b = isNotNull(materialize(a))`
-- reads `a` as a source: the block ends up advertising `a` as
-- `LowCardinality(Nullable(String))` while the column data is still on-disk
-- `Nullable(String)` and `materialize` fails with
-- `LOGICAL_ERROR: Unexpected return type from materialize`.
--
-- Variant 2 (`SELECT sum(b), any(a)`): both UPDATEs survive filtering. If the
-- skip set is a chain-wide union attached to every step, the earlier step
-- (UPDATE b) still skips `a` even though only the later step (UPDATE a)
-- overwrites it, so the same mismatch fires.
--
-- Variants 3 and 4 (sanity, from r3350748853 and r3350836226): both
-- assignments in a single `UPDATE` so they share a stage. `a` is then both
-- a target of the stage and a source of the same step's expression. The
-- bot warned this could re-trigger the type mismatch, first with
-- `isNotNull(materialize(a))` and then with `isNull(a)` for a stricter
-- function dispatch path. I could not reproduce a user-visible failure for
-- either shape: runtime function dispatch checks the actual column class
-- rather than the DAG-declared type, so `a` arriving as the on-disk
-- `ColumnNullable` does not trip the `isLowCardinalityNullable` path.
-- Locking the shapes in as sanity checks so any future change to action
-- analysis or to the per-step skip set has to re-evaluate them.

DROP TABLE IF EXISTS t_filtered_chain_isnotnull SYNC;

CREATE TABLE t_filtered_chain_isnotnull
(
    id UInt64,
    a Nullable(String),
    b UInt8
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_filtered_chain_isnotnull
SELECT number, if(number % 2 = 0, NULL, toString(number)), 0
FROM numbers(100);

SYSTEM STOP MERGES t_filtered_chain_isnotnull;

ALTER TABLE t_filtered_chain_isnotnull
    UPDATE b = isNotNull(materialize(a)) WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;

ALTER TABLE t_filtered_chain_isnotnull
    UPDATE a = 'x' WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;

ALTER TABLE t_filtered_chain_isnotnull
    MODIFY COLUMN a LowCardinality(Nullable(String))
    SETTINGS mutations_sync = 0, alter_sync = 0;

-- Variant 1: `filterMutationCommands` drops the later `UPDATE a`.
SELECT sum(b)
FROM t_filtered_chain_isnotnull
SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;

-- Variant 2: both `UPDATE`s survive filtering, exercises the per-step skip set.
SELECT sum(b), any(a)
FROM t_filtered_chain_isnotnull
SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;

DROP TABLE t_filtered_chain_isnotnull SYNC;

-- Variant 3: combined `UPDATE b = ..., a = ...` in a single stage.
DROP TABLE IF EXISTS t_combined_assignments SYNC;

CREATE TABLE t_combined_assignments
(
    id UInt64,
    a Nullable(String),
    b UInt8
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_combined_assignments
SELECT number, if(number % 2 = 0, NULL, toString(number)), 0
FROM numbers(100);

SYSTEM STOP MERGES t_combined_assignments;

ALTER TABLE t_combined_assignments
    UPDATE b = isNotNull(materialize(a)), a = 'x' WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;

ALTER TABLE t_combined_assignments
    MODIFY COLUMN a LowCardinality(Nullable(String))
    SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT sum(b), any(a)
FROM t_combined_assignments
SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;

DROP TABLE t_combined_assignments SYNC;

-- Variant 4: same shape as variant 3 but using `isNull(a)` for a stricter
-- function-dispatch path. Runtime dispatch still checks the actual column
-- class so the `isLowCardinalityNullable` branch does not fire.
DROP TABLE IF EXISTS t_combined_assignments_isnull SYNC;

CREATE TABLE t_combined_assignments_isnull
(
    id UInt64,
    a Nullable(String),
    b UInt8
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_combined_assignments_isnull
SELECT number, if(number % 2 = 0, NULL, toString(number)), 0
FROM numbers(100);

SYSTEM STOP MERGES t_combined_assignments_isnull;

ALTER TABLE t_combined_assignments_isnull
    UPDATE b = isNull(a), a = 'x' WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;

ALTER TABLE t_combined_assignments_isnull
    MODIFY COLUMN a LowCardinality(Nullable(String))
    SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT sum(b), any(a)
FROM t_combined_assignments_isnull
SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;

DROP TABLE t_combined_assignments_isnull SYNC;

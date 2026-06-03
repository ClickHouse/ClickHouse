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

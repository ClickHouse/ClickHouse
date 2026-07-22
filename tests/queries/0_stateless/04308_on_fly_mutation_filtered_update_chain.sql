-- Regression test from `clickhouse-gh[bot]` review on PR #105847
-- (https://github.com/ClickHouse/ClickHouse/pull/105847#discussion_r3346245939).
--
-- `filterMutationCommands` may drop a later `UPDATE` whose target the current
-- query does not need. If the on-fly skip set were built from the original
-- `mutation_commands` list it would still contain that dropped target, and an
-- earlier surviving step that reads it as a source would see the on-disk type
-- while the block already advertises the post-`MODIFY` type. The fix derives
-- the skip set from the chain that `filterMutationCommands` actually returns.
--
-- Scenario: `UPDATE b = isNotNull(materialize(a))`, then `UPDATE a = 'x'`,
-- then `MODIFY COLUMN a LowCardinality(Nullable(String))`. Reading only `b`
-- drops the `UPDATE a`. Without the fix the surviving `UPDATE b` action sees
-- `a` advertised as `LowCardinality(Nullable(String))` while it is still
-- on-disk `Nullable(String)`, and `materialize` fails with
-- `LOGICAL_ERROR: Unexpected return type from materialize`.

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

SELECT sum(b)
FROM t_filtered_chain_isnotnull
SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;

DROP TABLE t_filtered_chain_isnotnull SYNC;

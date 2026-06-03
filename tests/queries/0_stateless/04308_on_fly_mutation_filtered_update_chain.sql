-- Regression test from `clickhouse-gh[bot]` review on PR #105847
-- (https://github.com/ClickHouse/ClickHouse/pull/105847#discussion_r3350248306).
--
-- `filterMutationCommands` drops the later `UPDATE a` because the read does
-- not need `a` directly, but the surviving earlier `UPDATE b = isNotNull(materialize(a))`
-- still reads `a` as a source. If `columns_overwritten_by_chain` is built from
-- the original `mutation_commands` list (not the filtered one), `a` is excluded
-- from `performRequiredConversions`, the block ends up advertising `a` as
-- `LowCardinality(Nullable(String))` while the column data is still on-disk
-- `Nullable(String)`, and `materialize` fails with `LOGICAL_ERROR: Unexpected
-- return type from materialize`.

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

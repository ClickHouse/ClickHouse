-- Sanity check for a concern raised by `clickhouse-gh[bot]` in the review of
-- PR #105847 (https://github.com/ClickHouse/ClickHouse/pull/105847#discussion_r3346245939).
--
-- The bot pointed out that `columns_overwritten_by_chain` is computed from the
-- full `mutation_commands` list, while `filterMutationCommands` may drop a
-- later `UPDATE` whose target the current query does not need. The dropped
-- `UPDATE`'s target column would then still be excluded from
-- `performRequiredConversions`, even though no surviving step writes it.
-- In theory, that recreates the type/header mismatch the PR set out to fix
-- when the dropped target is also affected by a pending `MODIFY COLUMN`.
--
-- I could not reproduce the user-visible failure with this exact shape on
-- master: every query I tried (`SELECT b`, `SELECT b ORDER BY ...`,
-- `INSERT ... SELECT b`, `SELECT b WHERE a LIKE '%'`, `SELECT b ORDER BY a`,
-- `length(b)`, `INSERT ... Native`, etc.) returns the correct result. The
-- runtime path seems to handle the mixed LowCardinality/String case
-- transparently. Filing the test anyway so the next person who touches
-- `AlterConversions::getMutationSteps` or `filterMutationCommands` has to
-- re-evaluate this scenario.

DROP TABLE IF EXISTS t_filtered_chain SYNC;

CREATE TABLE t_filtered_chain (id UInt64, a String, b String)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_filtered_chain SELECT number,       's' || toString(number % 5), 'init' FROM numbers(100);
INSERT INTO t_filtered_chain SELECT number + 100, 's' || toString(number % 5), 'init' FROM numbers(100);

SYSTEM STOP MERGES t_filtered_chain;

-- The chain: read `a` to compute `b`, then overwrite `a`, then change `a`'s type.
ALTER TABLE t_filtered_chain UPDATE b = concat(a, '') WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_filtered_chain UPDATE a = upper(a) WHERE 1 = 1
    SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_filtered_chain MODIFY COLUMN a LowCardinality(String)
    SETTINGS mutations_sync = 0, alter_sync = 0;

-- Reading only `b` is what `filterMutationCommands` would prune the second
-- `UPDATE` of `a` for. This is the exact query shape from the bot's example.
SELECT b FROM t_filtered_chain
ORDER BY b
LIMIT 5000
SETTINGS apply_mutations_on_fly = 1, query_plan_max_limit_for_lazy_materialization = 10
FORMAT Null;

-- An aggregation that touches `b` only (no projection that would force `a`
-- through the output schema).
SELECT count() FROM t_filtered_chain
WHERE length(b) > 0
SETTINGS apply_mutations_on_fly = 1
FORMAT Null;

-- Final sanity: full read still sees the post-`MODIFY` type for `a`. With the
-- second `UPDATE` of `a` kept by `filterMutationCommands` in this query,
-- this is the standard happy path.
SELECT count(), groupUniqArray(toTypeName(a)), groupUniqArray(toTypeName(b))
FROM t_filtered_chain
SETTINGS apply_mutations_on_fly = 1
FORMAT TSV;

DROP TABLE t_filtered_chain SYNC;

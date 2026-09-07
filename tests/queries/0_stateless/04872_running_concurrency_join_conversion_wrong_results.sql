-- Regression test: `runningConcurrency` must report itself as non-deterministic in the scope of
-- a query, otherwise optimizer passes that rely on that predicate move it across a JOIN-kind
-- rewrite and it observes a different row set, returning wrong results.

-- A running function reads the physical row order, so the join order has to be fixed for any
-- statement about the row set it sees to be well defined.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS events_04872;
DROP TABLE IF EXISTS keys_04872;

CREATE TABLE events_04872 (a UInt64, s DateTime, e DateTime) ENGINE = Memory;
CREATE TABLE keys_04872 (b UInt64) ENGINE = Memory;

INSERT INTO events_04872 VALUES (1, '2020-01-01 00:00:00', '2020-01-01 00:00:10'), (2, '2020-01-01 00:00:05', '2020-01-01 00:00:20'), (3, '2020-01-01 00:00:06', '2020-01-01 00:00:07');
INSERT INTO keys_04872 VALUES (2);

-- Ground truth: the running value is computed in a subquery, so no rewrite can change the row
-- set it sees. Every query below must agree with this answer.
SELECT 'ground truth', count()
FROM (SELECT t.a FROM (SELECT a, runningConcurrency(s, e) AS rc FROM events_04872) AS t
      LEFT JOIN keys_04872 ON t.a = keys_04872.b
      WHERE t.rc = 2 AND keys_04872.b > 1);

-- The query under test, at default settings. Returned 0 before the fix.
SELECT 'carrier', count()
FROM (SELECT a FROM events_04872 LEFT JOIN keys_04872 ON events_04872.a = keys_04872.b
      WHERE runningConcurrency(s, e) = 2 AND keys_04872.b > 1);

-- Same, with the JOIN-kind conversion explicitly enabled and disabled: both must agree.
SELECT 'carrier convert=1', count()
FROM (SELECT a FROM events_04872 LEFT JOIN keys_04872 ON events_04872.a = keys_04872.b
      WHERE runningConcurrency(s, e) = 2 AND keys_04872.b > 1)
SETTINGS query_plan_convert_outer_join_to_inner_join = 1;

SELECT 'carrier convert=0', count()
FROM (SELECT a FROM events_04872 LEFT JOIN keys_04872 ON events_04872.a = keys_04872.b
      WHERE runningConcurrency(s, e) = 2 AND keys_04872.b > 1)
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

-- The other conversion this guard protects: ANY strictness becomes SEMI/ANTI. Pinned at 1 and 0
-- for the same reason as the rows above: the setting is randomized off on some runs, and an
-- unpinned row would silently stop exercising the pass.
SELECT 'carrier any', count()
FROM (SELECT a FROM events_04872 ANY LEFT JOIN keys_04872 ON events_04872.a = keys_04872.b
      WHERE runningConcurrency(s, e) = 2 AND keys_04872.b > 1)
SETTINGS query_plan_convert_any_join_to_semi_or_anti_join = 1;

SELECT 'carrier any convert=0', count()
FROM (SELECT a FROM events_04872 ANY LEFT JOIN keys_04872 ON events_04872.a = keys_04872.b
      WHERE runningConcurrency(s, e) = 2 AND keys_04872.b > 1)
SETTINGS query_plan_convert_any_join_to_semi_or_anti_join = 0;

-- The remaining two accepted argument types, which take separate execution paths. Pinned at 1 for
-- the same reason as the rows above: the setting is randomized off on some runs, and an unpinned
-- row would silently stop exercising the pass.
CREATE TABLE events_date_04872 (a UInt64, s Date, e Date) ENGINE = Memory;
CREATE TABLE events_dt64_04872 (a UInt64, s DateTime64(3), e DateTime64(3)) ENGINE = Memory;
INSERT INTO events_date_04872 VALUES (1, '2020-01-01', '2020-01-11'), (2, '2020-01-05', '2020-01-21'), (3, '2020-01-06', '2020-01-07');
INSERT INTO events_dt64_04872 VALUES (1, '2020-01-01 00:00:00.000', '2020-01-01 00:00:10.000'), (2, '2020-01-01 00:00:05.000', '2020-01-01 00:00:20.000'), (3, '2020-01-01 00:00:06.000', '2020-01-01 00:00:07.000');

SELECT 'carrier date', count()
FROM (SELECT a FROM events_date_04872 LEFT JOIN keys_04872 ON events_date_04872.a = keys_04872.b
      WHERE runningConcurrency(s, e) = 2 AND keys_04872.b > 1)
SETTINGS query_plan_convert_outer_join_to_inner_join = 1;

SELECT 'carrier datetime64', count()
FROM (SELECT a FROM events_dt64_04872 LEFT JOIN keys_04872 ON events_dt64_04872.a = keys_04872.b
      WHERE runningConcurrency(s, e) = 2 AND keys_04872.b > 1)
SETTINGS query_plan_convert_outer_join_to_inner_join = 1;

-- Control: a pure predicate selecting the same row. Already correct before the fix, so a
-- regression here means the fixture, not the fix, changed.
SELECT 'control pure', count()
FROM (SELECT a FROM events_04872 LEFT JOIN keys_04872 ON events_04872.a = keys_04872.b
      WHERE (a + 0) = 2 AND keys_04872.b > 1);

-- Control: a sibling running function that already declares itself non-deterministic in the
-- scope of the query. Also correct before the fix, which is what makes the carrier row above a
-- statement about the declaration and not about the JOIN rewrite itself.
SELECT 'control sibling', count()
FROM (SELECT a FROM events_04872 LEFT JOIN keys_04872 ON events_04872.a = keys_04872.b
      WHERE rowNumberInAllBlocks() = 1 AND keys_04872.b > 1);

-- The declaration as reported to users. This column reflects `isDeterministic`, which must be 0
-- for a running function alongside `isDeterministicInScopeOfQuery`.
SELECT name, deterministic FROM system.functions
WHERE name IN ('runningConcurrency', 'rowNumberInAllBlocks') ORDER BY name;

DROP TABLE events_04872;
DROP TABLE events_date_04872;
DROP TABLE events_dt64_04872;
DROP TABLE keys_04872;

-- A key expression must be deterministic, so this function is refused for one on every path,
-- including the attach path an upgraded server takes for a table an earlier version accepted.
-- `ATTACH TABLE` with an inline schema is the only shape reaching the storage constructor in
-- attach mode from a query, and only an Ordinary database parses it.
SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;

ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.legacy_sorting_key
    (a UInt64, s DateTime, e DateTime) ENGINE = MergeTree
    ORDER BY runningConcurrency(s, e); -- { serverError BAD_ARGUMENTS }
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.legacy_partition_key
    (a UInt64, s DateTime, e DateTime) ENGINE = MergeTree
    PARTITION BY runningConcurrency(s, e) ORDER BY a; -- { serverError BAD_ARGUMENTS }

-- Unlike a key, a secondary index is exempt on attach, so a table an earlier version created
-- still loads. The key here is deliberately deterministic, so only the index is under test.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.legacy_index
    (a UInt64, s DateTime, e DateTime,
     INDEX i runningConcurrency(s, e) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY a;
SELECT 'legacy index attaches', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.legacy_index;

-- A text index transform is validated on attach, unlike the index expression above, so a table
-- an earlier version accepted does not come back. The transform is wrapped in `toString` because
-- it must return a String; without the wrapper the return type is refused first and the
-- determinism check is never reached.
SET allow_experimental_full_text_index = 1;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.legacy_text_preprocessor
    (a UInt64, val String,
     INDEX i(val) TYPE text(tokenizer = 'splitByNonAlpha',
        preprocessor = toString(runningConcurrency(toDateTime(val), toDateTime(val)))))
    ENGINE = MergeTree ORDER BY a; -- { serverError INCORRECT_QUERY }
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.legacy_text_postprocessor
    (a UInt64, val String,
     INDEX i(val) TYPE text(tokenizer = 'splitByNonAlpha',
        postprocessor = toString(runningConcurrency(toDateTime(val), toDateTime(val)))))
    ENGINE = MergeTree ORDER BY a; -- { serverError INCORRECT_QUERY }

-- Negative control: a deterministic transform still attaches, so the two rows above are refused
-- for the function they name and not for using a transform at all.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.legacy_text_plain
    (a UInt64, val String,
     INDEX i(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val)))
    ENGINE = MergeTree ORDER BY a;
SELECT 'legacy text index attaches', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.legacy_text_plain;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.rejected
    (a UInt64, s DateTime, e DateTime) ENGINE = MergeTree
    ORDER BY runningConcurrency(s, e); -- { serverError BAD_ARGUMENTS }
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.rejected
    (a UInt64, s DateTime, e DateTime) ENGINE = MergeTree
    PARTITION BY runningConcurrency(s, e) ORDER BY a; -- { serverError BAD_ARGUMENTS }

-- `MODIFY ORDER BY` may only extend the key with newly added columns, and such a column may not
-- carry a default, so both columns are added in the same statement without one.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.altered (a UInt64) ENGINE = MergeTree ORDER BY a;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.altered
    ADD COLUMN s2 DateTime, ADD COLUMN e2 DateTime,
    MODIFY ORDER BY (a, runningConcurrency(s2, e2)); -- { serverError BAD_ARGUMENTS }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- A secondary index expression must be deterministic too, but unlike a key it is validated only
-- when the statement introduces it, which is why the attach above keeps loading.
CREATE TABLE idx_04872 (a UInt64, s DateTime, e DateTime,
    INDEX i runningConcurrency(s, e) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY a; -- { serverError BAD_ARGUMENTS }

CREATE TABLE idx_04872 (a UInt64, s DateTime, e DateTime) ENGINE = MergeTree ORDER BY a;
ALTER TABLE idx_04872
    ADD INDEX i runningConcurrency(s, e) TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }
DROP TABLE idx_04872;

-- Sibling control: an index over an already-non-deterministic running function is refused the same
-- way, so the rows above state what a correct running function does today.
CREATE TABLE idx_sibling_04872 (a UInt64, s DateTime, e DateTime,
    INDEX i rowNumberInAllBlocks() TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY a; -- { serverError BAD_ARGUMENTS }

-- Negative control: a deterministic index expression is still accepted.
CREATE TABLE idx_plain_04872 (a UInt64, s DateTime, e DateTime,
    INDEX i s TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a;
DROP TABLE idx_plain_04872;

-- A TTL expression must be deterministic too, with `allow_suspicious_ttl_expressions` as the
-- escape hatch. Table-scoped, so these rows stay parallel-safe.
CREATE TABLE ttl_04872 (a UInt64, s DateTime, e DateTime) ENGINE = MergeTree ORDER BY a
    TTL s + toIntervalSecond(runningConcurrency(s, e)); -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_ttl_expressions = 1;
CREATE TABLE ttl_04872 (a UInt64, s DateTime, e DateTime) ENGINE = MergeTree ORDER BY a
    TTL s + toIntervalSecond(runningConcurrency(s, e));
DROP TABLE ttl_04872;
SET allow_suspicious_ttl_expressions = 0;

-- Sibling control: the same expression with a function that is already non-deterministic is
-- refused on both sides of this change, so the row above is a statement about the declaration.
CREATE TABLE ttl_sibling_04872 (a UInt64, s DateTime, e DateTime) ENGINE = MergeTree ORDER BY a
    TTL s + toIntervalSecond(rowNumberInAllBlocks()); -- { serverError BAD_ARGUMENTS }

-- Negative control: a deterministic TTL expression is still accepted.
CREATE TABLE ttl_plain_04872 (a UInt64, s DateTime, e DateTime) ENGINE = MergeTree ORDER BY a
    TTL s + toIntervalSecond(1);
DROP TABLE ttl_plain_04872;

-- Reading a table with a pending mutation that assigns the function is refused while
-- `apply_mutations_on_fly` is on: the on-fly path would have to evaluate it per part. The
-- mutation itself is accepted, and once it materialises the table reads normally again, so the
-- refusal lasts only as long as the mutation is unfinished. There is no setting that skips it.
CREATE TABLE onfly_04872 (a UInt64, s DateTime, e DateTime, v UInt32) ENGINE = MergeTree ORDER BY a;
SYSTEM STOP MERGES onfly_04872;
INSERT INTO onfly_04872 VALUES (1, '2020-01-01 00:00:00', '2020-01-01 00:00:10', 0);

SET apply_mutations_on_fly = 1;
ALTER TABLE onfly_04872 UPDATE v = runningConcurrency(s, e) WHERE 1;
SELECT v FROM onfly_04872; -- { serverError BAD_ARGUMENTS }

-- `allow_nondeterministic_mutations` guards mutation submission on `Replicated*` tables, not
-- this read, so it does not help here.
SELECT v FROM onfly_04872 SETTINGS allow_nondeterministic_mutations = 1; -- { serverError BAD_ARGUMENTS }

-- Negative control: with the on-fly path off, the same pending mutation is invisible to the read.
SELECT 'onfly off', v FROM onfly_04872 SETTINGS apply_mutations_on_fly = 0;

-- The refusal ends with the mutation.
SYSTEM START MERGES onfly_04872;
ALTER TABLE onfly_04872 UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'onfly after materialisation', v FROM onfly_04872;
SET apply_mutations_on_fly = 0;
DROP TABLE onfly_04872;

-- Sibling control for the same read path.
CREATE TABLE onfly_sibling_04872 (a UInt64, s DateTime, e DateTime, v UInt32) ENGINE = MergeTree ORDER BY a;
SYSTEM STOP MERGES onfly_sibling_04872;
INSERT INTO onfly_sibling_04872 VALUES (1, '2020-01-01 00:00:00', '2020-01-01 00:00:10', 0);
ALTER TABLE onfly_sibling_04872 UPDATE v = rowNumberInAllBlocks() WHERE 1;
SELECT v FROM onfly_sibling_04872 SETTINGS apply_mutations_on_fly = 1; -- { serverError BAD_ARGUMENTS }
DROP TABLE onfly_sibling_04872;

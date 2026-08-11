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

-- The remaining two accepted argument types, which take separate execution paths.
CREATE TABLE events_date_04872 (a UInt64, s Date, e Date) ENGINE = Memory;
CREATE TABLE events_dt64_04872 (a UInt64, s DateTime64(3), e DateTime64(3)) ENGINE = Memory;
INSERT INTO events_date_04872 VALUES (1, '2020-01-01', '2020-01-11'), (2, '2020-01-05', '2020-01-21'), (3, '2020-01-06', '2020-01-07');
INSERT INTO events_dt64_04872 VALUES (1, '2020-01-01 00:00:00.000', '2020-01-01 00:00:10.000'), (2, '2020-01-01 00:00:05.000', '2020-01-01 00:00:20.000'), (3, '2020-01-01 00:00:06.000', '2020-01-01 00:00:07.000');

SELECT 'carrier date', count()
FROM (SELECT a FROM events_date_04872 LEFT JOIN keys_04872 ON events_date_04872.a = keys_04872.b
      WHERE runningConcurrency(s, e) = 2 AND keys_04872.b > 1);

SELECT 'carrier datetime64', count()
FROM (SELECT a FROM events_dt64_04872 LEFT JOIN keys_04872 ON events_dt64_04872.a = keys_04872.b
      WHERE runningConcurrency(s, e) = 2 AND keys_04872.b > 1);

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

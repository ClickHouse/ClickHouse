-- Tags: no-old-analyzer
-- The fix lives in the analyzer; the old analyzer rejects these query
-- shapes with a different error before the planner runs, so the bug cannot
-- manifest there.

DROP TABLE IF EXISTS t_04303;
CREATE TABLE t_04303 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_04303 VALUES (1, 10), (2, 20);

-- Non-correlated EXISTS over an unaliased real-table subquery: must work.
ALTER TABLE t_04303 UPDATE v = 0 WHERE exists((SELECT * FROM system.one)) SETTINGS mutations_sync = 2;
SELECT id, v FROM t_04303 ORDER BY id;

DROP TABLE IF EXISTS test_updates_04303;
CREATE TABLE test_updates_04303 (id UInt64, dynamic Dynamic)
    ENGINE = MergeTree ORDER BY id;
INSERT INTO test_updates_04303 VALUES (1, 'a'), (2, 'b');
ALTER TABLE test_updates_04303
    (UPDATE dynamic = NULL WHERE exists((SELECT DISTINCT *, currentDatabase() > database)))
    SETTINGS mutations_sync = 2;
SELECT id, isNull(dynamic) FROM test_updates_04303 ORDER BY id;

-- A NOT EXISTS variant, which the analyzer rewrites to `not exists((subq))`
-- and then expands the same way: must also work.
DROP TABLE IF EXISTS t_04303_b;
CREATE TABLE t_04303_b (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_04303_b VALUES (1, 10), (2, 20);
ALTER TABLE t_04303_b UPDATE v = 1
    WHERE NOT exists((SELECT * FROM system.numbers WHERE number > 1000000 LIMIT 1))
    SETTINGS mutations_sync = 2;
SELECT id, v FROM t_04303_b ORDER BY id;

-- Correlated subqueries in mutations are not supported and must be rejected
-- with a clean NOT_IMPLEMENTED, never with LOGICAL_ERROR / SIGABRT.

DROP TABLE IF EXISTS t_04303_corr;
CREATE TABLE t_04303_corr (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_04303_corr VALUES (1, 10), (2, 20);

ALTER TABLE t_04303_corr UPDATE v = 99
    WHERE exists((SELECT 1 WHERE id > 0))
    SETTINGS mutations_sync = 2; -- { serverError NOT_IMPLEMENTED }

ALTER TABLE t_04303_corr UPDATE v = 99
    WHERE exists((SELECT * FROM numbers(10) WHERE number = id))
    SETTINGS mutations_sync = 2; -- { serverError NOT_IMPLEMENTED }

ALTER TABLE t_04303_corr DELETE
    WHERE exists((SELECT 1 WHERE v < 100))
    SETTINGS mutations_sync = 2; -- { serverError NOT_IMPLEMENTED }

-- Issue #106649 shape: ALTER UPDATE WHERE id IN (subquery joining two
-- subquery-wrapped derived tables that both project `id`). The mutation path
-- used to abort with `Logical error: Column identifier id is already
-- registered`; with the fix, the detached subquery in `PreparedSets` gets
-- unique `__tableN` aliases per source node and the mutation runs.
DROP TABLE IF EXISTS gh_04303;
DROP TABLE IF EXISTS prs_04303;
DROP TABLE IF EXISTS edges_04303;
CREATE TABLE gh_04303    (id UInt64, x UInt8) ENGINE = MergeTree ORDER BY id;
CREATE TABLE prs_04303   (id UInt64)          ENGINE = MergeTree ORDER BY id;
CREATE TABLE edges_04303 (from_id UInt64, to_id UInt64) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE gh_04303 UPDATE x = 1 WHERE id IN (
    SELECT i.id
    FROM (SELECT id FROM gh_04303)  AS i
    JOIN edges_04303 AS r           ON r.to_id = i.id
    JOIN (SELECT id FROM prs_04303) AS pr ON pr.id = r.from_id
) SETTINGS mutations_sync = 2;
SELECT count() FROM gh_04303;

DROP TABLE t_04303;
DROP TABLE test_updates_04303;
DROP TABLE t_04303_b;
DROP TABLE t_04303_corr;
DROP TABLE gh_04303;
DROP TABLE prs_04303;
DROP TABLE edges_04303;

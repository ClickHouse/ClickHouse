-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

-- { echoOn }

DROP VIEW IF EXISTS view_ifnull;
DROP VIEW IF EXISTS view_coalesce;
DROP VIEW IF EXISTS view_assume;
DROP TABLE IF EXISTS test;

SET session_timezone = 'UTC';

CREATE TABLE test
(
    id UInt64,
    ts Nullable(DateTime64(3)),
    INDEX idx_ts ts TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test VALUES
    (1, toDateTime64('2024-12-28 00:00:00', 3)),
    (2, toDateTime64('2024-12-29 00:00:00', 3)),
    (3, toDateTime64('2024-12-30 00:00:00', 3)),
    (4, toDateTime64('2025-01-01 00:00:00', 3)),
    (5, toDateTime64('2025-01-02 00:00:00', 3)),
    (6, toDateTime64('2025-01-03 00:00:00', 3)),
    (7, toDateTime64('2025-01-04 00:00:00', 3)),
    (8, toDateTime64('2025-01-05 00:00:00', 3));

CREATE VIEW view_assume AS
SELECT
    id,
    assumeNotNull(ts) AS ts
FROM test;

CREATE VIEW view_coalesce AS
SELECT
    id,
    coalesce(ts, toDateTime64('1970-01-01 00:00:00', 3)) AS ts
FROM test;

CREATE VIEW view_ifnull AS
SELECT
    id,
    ifNull(ts, toDateTime64('1970-01-01 00:00:00', 3)) AS ts
FROM test;

SELECT count()
FROM test
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3);

EXPLAIN indexes = 1
SELECT count()
FROM test
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3);

SELECT 'assumeNotNull';

SELECT count()
FROM view_assume
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3);

EXPLAIN indexes = 1
SELECT count()
FROM view_assume
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3);

SELECT 'coalesce';

SELECT count()
FROM view_coalesce
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3);

EXPLAIN indexes = 1
SELECT count()
FROM view_coalesce
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3);

SELECT 'ifNull';
SELECT count()
FROM view_ifnull
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3);

EXPLAIN indexes = 1
SELECT count()
FROM view_ifnull
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3);

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

SELECT *
FROM test
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3)
ORDER BY id;

EXPLAIN indexes = 1
SELECT count()
FROM test
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3);

SELECT 'assumeNotNull';

SELECT *
FROM view_assume
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3)
ORDER BY id;

EXPLAIN indexes = 1
SELECT count()
FROM view_assume
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3);

SELECT 'coalesce';

SELECT *
FROM view_coalesce
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3)
ORDER BY id;

EXPLAIN indexes = 1
SELECT count()
FROM view_coalesce
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3);

SELECT 'ifNull';
SELECT *
FROM view_ifnull
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3)
ORDER BY id;

EXPLAIN indexes = 1
SELECT count()
FROM view_ifnull
WHERE ts >= toDateTime64('2025-01-01 00:00:00', 3);

-- Nullable type, bounded max (right is not NULL/+Inf) => monotonicity is allowed.
DROP TABLE IF EXISTS test_non_null;

CREATE TABLE test_non_null
(
    ts Nullable(DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY ts
SETTINGS index_granularity = 1, allow_nullable_key = 1;

INSERT INTO test_non_null VALUES
    (toDateTime64('2026-01-01 00:00:00', 3)),
    (toDateTime64('2026-01-02 00:00:00', 3)),
    (toDateTime64('2026-01-03 00:00:00', 3)),
    (toDateTime64('2026-01-04 00:00:00', 3)),
    (toDateTime64('2026-01-05 00:00:00', 3));

SELECT 'assumeNotNull non-null max';

SELECT *
FROM test_non_null
WHERE assumeNotNull(ts) <= toDateTime64('2025-01-01 00:00:00', 3)
ORDER BY ALL;

EXPLAIN indexes = 1
SELECT count()
FROM test_non_null
WHERE assumeNotNull(ts) <= toDateTime64('2025-01-01 00:00:00', 3);

SELECT 'coalesce non-null max';

SELECT *
FROM test_non_null
WHERE coalesce(ts, toDateTime64('1970-01-01 00:00:00', 3)) <= toDateTime64('2025-01-01 00:00:00', 3)
ORDER BY ALL;

EXPLAIN indexes = 1
SELECT count()
FROM test_non_null
WHERE coalesce(ts, toDateTime64('1970-01-01 00:00:00', 3)) <= toDateTime64('2025-01-01 00:00:00', 3);

SELECT 'ifNull non-null max';

SELECT *
FROM test_non_null
WHERE ifNull(ts, toDateTime64('1970-01-01 00:00:00', 3)) <= toDateTime64('2025-01-01 00:00:00', 3)
ORDER BY ALL;

EXPLAIN indexes = 1
SELECT count()
FROM test_non_null
WHERE ifNull(ts, toDateTime64('1970-01-01 00:00:00', 3)) <= toDateTime64('2025-01-01 00:00:00', 3);

-- Nullable type, +Inf max due to NULLs (right is NULL/+Inf) => monotonicity is disabled.
DROP TABLE IF EXISTS test_null;

CREATE TABLE test_null
(
    ts Nullable(DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY ts
SETTINGS index_granularity = 1, allow_nullable_key = 1;

INSERT INTO test_null VALUES
    (toDateTime64('2026-01-01 00:00:00', 3)),
    (toDateTime64('2026-01-02 00:00:00', 3)),
    (NULL),
    (toDateTime64('2026-01-03 00:00:00', 3)),
    (toDateTime64('2026-01-04 00:00:00', 3));

SELECT 'assumeNotNull null max';

SELECT *
FROM test_null
WHERE assumeNotNull(ts) <= toDateTime64('2025-01-01 00:00:00', 3)
ORDER BY ALL;

EXPLAIN indexes = 1
SELECT count()
FROM test_null
WHERE assumeNotNull(ts) <= toDateTime64('2025-01-01 00:00:00', 3);

SELECT 'coalesce null max';

SELECT *
FROM test_null
WHERE coalesce(ts, toDateTime64('1970-01-01 00:00:00', 3)) <= toDateTime64('2025-01-01 00:00:00', 3)
ORDER BY ALL;

EXPLAIN indexes = 1
SELECT count()
FROM test_null
WHERE coalesce(ts, toDateTime64('1970-01-01 00:00:00', 3)) <= toDateTime64('2025-01-01 00:00:00', 3);

SELECT 'ifNull null max';

SELECT *
FROM test_null
WHERE ifNull(ts, toDateTime64('1970-01-01 00:00:00', 3)) <= toDateTime64('2025-01-01 00:00:00', 3)
ORDER BY ALL;

EXPLAIN indexes = 1
SELECT count()
FROM test_null
WHERE ifNull(ts, toDateTime64('1970-01-01 00:00:00', 3)) <= toDateTime64('2025-01-01 00:00:00', 3);


DROP TABLE IF EXISTS test_null_rev;

CREATE TABLE test_null_rev
(
    ts Nullable(DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY (ts DESC)
SETTINGS index_granularity = 1, allow_nullable_key = 1, allow_experimental_reverse_key = 1;

INSERT INTO test_null_rev VALUES
    (toDateTime64('2026-01-01 00:00:00', 3)),
    (toDateTime64('2026-01-02 00:00:00', 3)),
    (NULL),
    (toDateTime64('2026-01-03 00:00:00', 3)),
    (toDateTime64('2026-01-04 00:00:00', 3));

SELECT 'assumeNotNull null max';

SELECT *
FROM test_null_rev
WHERE assumeNotNull(ts) <= toDateTime64('2025-01-01 00:00:00', 3)
ORDER BY ALL;

EXPLAIN indexes = 1
SELECT count()
FROM test_null_rev
WHERE assumeNotNull(ts) <= toDateTime64('2025-01-01 00:00:00', 3);

SELECT 'coalesce null max';

SELECT *
FROM test_null_rev
WHERE coalesce(ts, toDateTime64('1970-01-01 00:00:00', 3)) <= toDateTime64('2025-01-01 00:00:00', 3)
ORDER BY ALL;

EXPLAIN indexes = 1
SELECT count()
FROM test_null_rev
WHERE coalesce(ts, toDateTime64('1970-01-01 00:00:00', 3)) <= toDateTime64('2025-01-01 00:00:00', 3);

SELECT 'ifNull null max';

SELECT *
FROM test_null_rev
WHERE ifNull(ts, toDateTime64('1970-01-01 00:00:00', 3)) <= toDateTime64('2025-01-01 00:00:00', 3)
ORDER BY ALL;

EXPLAIN indexes = 1
SELECT count()
FROM test_null_rev
WHERE ifNull(ts, toDateTime64('1970-01-01 00:00:00', 3)) <= toDateTime64('2025-01-01 00:00:00', 3);


DROP TABLE IF EXISTS test_lc_left_inf;

SET allow_suspicious_low_cardinality_types = 1;
SET optimize_use_projections = 0;

CREATE TABLE test_lc_left_inf
(
    a UInt8,
    ts LowCardinality(Nullable(Int64))
)
ENGINE = MergeTree()
ORDER BY (a, ts)
SETTINGS index_granularity = 1, allow_nullable_key = 1;

INSERT INTO test_lc_left_inf VALUES
    (1, 0),
    (1, 0),
    (1, 0),
    (2, 0),
    (2, 0),
    (2, 2000);

SELECT 'assumeNotNull LowCardinality left bound';

SELECT *
FROM test_lc_left_inf
WHERE assumeNotNull(ts) >= 1000
ORDER BY ALL;

EXPLAIN indexes = 1
SELECT count()
FROM test_lc_left_inf
WHERE assumeNotNull(ts) >= 1000;

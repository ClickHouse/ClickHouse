-- Tags: no-replicated-database, no-parallel-replicas, no-parallel, no-random-merge-tree-settings
-- EXPLAIN output may differ

-- { echo }

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    ts DateTime,
    x UInt32
)
ENGINE = MergeTree
ORDER BY (toYYYYMM(ts), toDate(ts), ts)
SETTINGS index_granularity=1, add_minmax_index_for_numeric_columns=0;

INSERT INTO test
SELECT
    toDateTime('2026-01-01 00:00:00') + number * 3600,
    toUInt32(number)
FROM numbers(24 * 40);

SELECT count()
FROM test
WHERE ts >= toDateTime('2026-01-10 00:00:00')
  AND ts < toDateTime('2026-01-11 00:00:00');

EXPLAIN indexes = 1
SELECT count()
FROM test
WHERE ts >= toDateTime('2026-01-10 00:00:00')
  AND ts < toDateTime('2026-01-11 00:00:00');


SELECT count()
FROM test
WHERE ts IN (
    toDateTime('2026-01-10 00:00:00'),
    toDateTime('2026-01-10 01:00:00'),
    toDateTime('2026-02-05 00:00:00')
);

EXPLAIN indexes = 1
SELECT count()
FROM test
WHERE ts IN (
    toDateTime('2026-01-10 00:00:00'),
    toDateTime('2026-01-10 01:00:00'),
    toDateTime('2026-02-05 00:00:00')
);

SELECT count()
FROM test
WHERE has([
    toDateTime('2026-01-10 00:00:00'),
    toDateTime('2026-02-05 00:00:00')
], ts);


EXPLAIN indexes = 1
SELECT count()
FROM test
WHERE has([
    toDateTime('2026-01-10 00:00:00'),
    toDateTime('2026-02-05 00:00:00')
], ts);

SELECT count()
FROM test
WHERE toDate(ts) IN (
    toDate('2026-01-10'),
    toDate('2026-02-05')
);

EXPLAIN indexes = 1
SELECT count()
FROM test
WHERE toDate(ts) IN (
    toDate('2026-01-10'),
    toDate('2026-02-05')
);


SELECT count()
FROM test
WHERE ts IN (
    toDate('2026-01-10'),
    toDate('2026-02-05')
);

EXPLAIN indexes = 1
SELECT count()
FROM test
WHERE ts IN (
    toDate('2026-01-10'),
    toDate('2026-02-05')
);


SELECT count()
FROM test
WHERE ts IN (
    toDate('2026-01-10'),
    toDate('2026-02-05')
) OR ts <= toDateTime('2026-01-10 00:00:00')
OR  has([
    toDateTime('2026-01-10 00:00:00'),
    toDateTime('2026-02-05 00:00:00')
], ts) OR ts = toDateTime('2026-01-10 00:00:00');

EXPLAIN indexes = 1
SELECT count()
FROM test
WHERE ts IN (
    toDate('2026-01-10'),
    toDate('2026-02-05')
) OR ts <= toDateTime('2026-01-10 00:00:00')
OR  has([
    toDateTime('2026-01-10 00:00:00'),
    toDateTime('2026-02-05 00:00:00')
], ts) OR ts = toDateTime('2026-01-10 00:00:00');

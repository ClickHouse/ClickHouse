-- Projections should use ReadType: InOrder when ORDER BY matches projection's sorting key.
-- Previously, ReadType would be Default (https://github.com/ClickHouse/ClickHouse/issues/89453)
-- Tags: no-random-settings

SET optimize_read_in_order = 1, force_optimize_projection = 1;

DROP TABLE IF EXISTS test_03814_optimize;

CREATE TABLE test_03814_optimize (id UInt64, ts DateTime, v UInt64, PROJECTION p (SELECT * ORDER BY ts))
ENGINE = MergeTree() PARTITION BY toYYYYMM(ts) ORDER BY (id, toStartOfHour(ts));

INSERT INTO test_03814_optimize SELECT number, toDateTime('2025-01-01') + number, number FROM numbers(100);

SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT * FROM test_03814_optimize WHERE ts BETWEEN '2025-01-01' AND '2025-01-02' ORDER BY ts
)
WHERE explain LIKE '%ReadType:%';

SELECT id, ts, v FROM test_03814_optimize WHERE ts BETWEEN '2025-01-01' AND '2025-01-02' ORDER BY ts LIMIT 3;

DROP TABLE test_03814_optimize SYNC;

-- Same optimization when projection is not fully materialized (some parts have it, some don't)
DROP TABLE IF EXISTS test_03814_optimize_partial;
CREATE TABLE test_03814_optimize_partial (id UInt64, ts DateTime, v UInt64)
ENGINE = MergeTree() PARTITION BY toYYYYMM(ts) ORDER BY (id, toStartOfHour(ts));
SYSTEM STOP MERGES test_03814_optimize_partial;
INSERT INTO test_03814_optimize_partial SELECT number, toDateTime('2025-01-01') + number, number FROM numbers(50);
ALTER TABLE test_03814_optimize_partial ADD PROJECTION p (SELECT * ORDER BY ts);
INSERT INTO test_03814_optimize_partial SELECT number, toDateTime('2025-01-01') + number, number FROM numbers(50, 50);

SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT * FROM test_03814_optimize_partial WHERE ts BETWEEN '2025-01-01' AND '2025-01-02' ORDER BY ts
)
WHERE explain LIKE '%ReadType:%';

SELECT id, ts, v FROM test_03814_optimize_partial WHERE ts BETWEEN '2025-01-01' AND '2025-01-02' ORDER BY ts LIMIT 3;

DROP TABLE test_03814_optimize_partial SYNC;

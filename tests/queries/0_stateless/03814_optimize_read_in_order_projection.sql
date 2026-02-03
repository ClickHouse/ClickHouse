-- Projections should use ReadType: InOrder when ORDER BY matches projection's sorting key.
-- Previously, ReadType would be Default (https://github.com/ClickHouse/ClickHouse/issues/89453)
-- Tags: no-random-settings

SET optimize_read_in_order = 1, force_optimize_projection = 1;

DROP TABLE IF EXISTS t;

CREATE TABLE t (id UInt64, ts DateTime, v UInt64, PROJECTION p (SELECT * ORDER BY ts))
ENGINE = MergeTree() PARTITION BY toYYYYMM(ts) ORDER BY (id, toStartOfHour(ts));

INSERT INTO t SELECT number, toDateTime('2025-01-01') + number, number FROM numbers(100);

SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT * FROM t WHERE ts BETWEEN '2025-01-01' AND '2025-01-02' ORDER BY ts
)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%ReadType:%' OR explain LIKE '%Prefix sort%';

SELECT id, ts, v FROM t WHERE ts BETWEEN '2025-01-01' AND '2025-01-02' ORDER BY ts LIMIT 3;

DROP TABLE t;

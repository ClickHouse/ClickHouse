-- Regression test for assertion failure in removeUnusedColumns optimization
-- when indexHint with star expansion is used with FINAL on a ReplacingMergeTree.
-- The indexHint DAG has no real column dependencies (returns constant 1),
-- so the ExpressionStep reduces its inputs aggressively. But the child
-- ReadFromMergeTree with FINAL cannot reduce its output (needs sort key +
-- version columns for merging). This caused a "Block structure mismatch"
-- assertion in debug/sanitizer builds.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS mytable__fuzz_45;

CREATE TABLE mytable__fuzz_45
(
    `timestamp` UInt32,
    `insert_timestamp` LowCardinality(UInt64),
    `key` Nullable(UInt16),
    `value` Nullable(DateTime)
)
ENGINE = ReplacingMergeTree(insert_timestamp)
PRIMARY KEY (key, timestamp)
ORDER BY (key, timestamp)
SETTINGS allow_nullable_key = 1;

INSERT INTO mytable__fuzz_45 VALUES (1, 100, 5, '2020-01-01 00:00:00'), (2, 200, 5, '2020-01-02 00:00:00');

SELECT indexHint(isNotNull(100), indexHint(indexHint(indexHint(assumeNotNull(0), isNotNull(0)), 0), isNullable(0), isNull(isNull(0))), *), value
FROM mytable__fuzz_45 FINAL PREWHERE key = 5 ORDER BY ALL ASC NULLS FIRST;

DROP TABLE IF EXISTS mytable__fuzz_45;

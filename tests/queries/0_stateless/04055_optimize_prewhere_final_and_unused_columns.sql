-- When `optimizePrewhere` pushes filters into prewhere and then removes unused columns,
-- `ReadFromMergeTree` with FINAL may keep extra sort key columns needed for merging that
-- the parent step didn't request, causing a header mismatch.
SET query_plan_remove_unused_columns = 1;

DROP TABLE IF EXISTS t;

CREATE TABLE t
(
    id UInt64,
    update_ts DateTime,
    value UInt32
)
ENGINE = ReplacingMergeTree(update_ts)
PARTITION BY 0 * id
ORDER BY (update_ts, id);

INSERT INTO t
SELECT number, toDateTime('2020-01-01 00:00:00'), 1
FROM numbers(100);

SELECT count()
FROM t FINAL
WHERE (42 >= id)
  AND ('2019-01-01 00:00:00' <= update_ts)
  AND equals(and(8, 8), 1);

-- Test also with settings that caused the initial bug
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_move_to_prewhere_if_final = 1;

SELECT count()
FROM t FINAL
WHERE (42 >= id)
  AND ('2019-01-01 00:00:00' <= update_ts)
  AND equals(and(8, 8), 1);

-- Exact queries from fuzzer
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS t__fuzz_15;
CREATE TABLE t__fuzz_15
(
    id LowCardinality(UInt64),
    update_ts DateTime,
    value UInt32
)
ENGINE = ReplacingMergeTree(update_ts)
PARTITION BY 0 / id
ORDER BY tuple(update_ts);

INSERT INTO t__fuzz_15 SELECT number, toDateTime('2020-01-01 00:00:00'), 1 FROM numbers(100);

SELECT count()
FROM t__fuzz_15 FINAL
WHERE (42 >= id)
      AND (update_ts <= '2019-01-01 00:00:00')
      AND equals(and(8, 1024), materialize(2147483648));

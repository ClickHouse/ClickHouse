-- Regression test for FINAL LIMIT pushdown through StorageMerge / ReadFromMerge.
--
-- `SELECT ... FROM merge_tbl FINAL ORDER BY key LIMIT N SETTINGS optimize_final_limit_pushdown = 1`
-- must push the limit into the child MergeTree FINAL merge pipelines (visible as
-- `Description: limit N` in EXPLAIN PIPELINE), the results must stay correct, and the pushdown
-- must never corrupt results for engines that cannot early-terminate (ReplacingMergeTree) or
-- when a filter runs after FINAL.
--
-- Child tables use disjoint key ranges so that `ORDER BY key LIMIT N` over the Merge table is
-- deterministic (no ties across children); all checks are structured to print a single `1`.

SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_merge_agg_1;
DROP TABLE IF EXISTS t_merge_agg_2;
DROP TABLE IF EXISTS t_merge_agg;

CREATE TABLE t_merge_agg_1 (key UInt64, val AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY key;
CREATE TABLE t_merge_agg_2 (key UInt64, val AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY key;

INSERT INTO t_merge_agg_1 SELECT number, sumState(toUInt64(1)) FROM numbers(1000) GROUP BY number;
INSERT INTO t_merge_agg_1 SELECT number, sumState(toUInt64(10)) FROM numbers(1000) GROUP BY number;
INSERT INTO t_merge_agg_2 SELECT number + 1000, sumState(toUInt64(100)) FROM numbers(1000) GROUP BY number;
INSERT INTO t_merge_agg_2 SELECT number + 1000, sumState(toUInt64(1000)) FROM numbers(1000) GROUP BY number;

CREATE TABLE t_merge_agg AS t_merge_agg_1 ENGINE = Merge(currentDatabase(), '^t_merge_agg_[12]$');

-- The pushed-down limit must reach the child AggregatingMergeTree merge pipeline.
SELECT 'explain_merge_agg_on';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, finalizeAggregation(val)
    FROM t_merge_agg FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1, optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 5%';

-- With the setting off, no limit is pushed into the merge.
SELECT 'explain_merge_agg_off';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, finalizeAggregation(val)
    FROM t_merge_agg FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 0
)
WHERE explain LIKE '%Description: limit%';

-- Pushdown must not change the results.
SELECT 'correctness_agg';
SELECT
    arraySort((SELECT groupArray((key, finalizeAggregation(val))) FROM (SELECT key, val FROM t_merge_agg FINAL ORDER BY key LIMIT 5 SETTINGS optimize_final_limit_pushdown = 1)))
  = arraySort((SELECT groupArray((key, finalizeAggregation(val))) FROM (SELECT key, val FROM t_merge_agg FINAL ORDER BY key LIMIT 5 SETTINGS optimize_final_limit_pushdown = 0)));

-- A filter that runs after FINAL must not be truncated by the pushed-down limit.
SELECT 'correctness_agg_where';
SELECT
    arraySort((SELECT groupArray((key, finalizeAggregation(val))) FROM (SELECT key, val FROM t_merge_agg FINAL WHERE key % 3 = 0 ORDER BY key LIMIT 5 SETTINGS optimize_final_limit_pushdown = 1)))
  = arraySort((SELECT groupArray((key, finalizeAggregation(val))) FROM (SELECT key, val FROM t_merge_agg FINAL WHERE key % 3 = 0 ORDER BY key LIMIT 5 SETTINGS optimize_final_limit_pushdown = 0)));

DROP TABLE IF EXISTS t_merge_sum_1;
DROP TABLE IF EXISTS t_merge_sum_2;
DROP TABLE IF EXISTS t_merge_sum;

CREATE TABLE t_merge_sum_1 (key UInt64, value UInt64) ENGINE = SummingMergeTree ORDER BY key;
CREATE TABLE t_merge_sum_2 (key UInt64, value UInt64) ENGINE = SummingMergeTree ORDER BY key;

INSERT INTO t_merge_sum_1 SELECT number, 1 FROM numbers(1000);
INSERT INTO t_merge_sum_1 SELECT number, 10 FROM numbers(1000);
INSERT INTO t_merge_sum_2 SELECT number + 1000, 100 FROM numbers(1000);
INSERT INTO t_merge_sum_2 SELECT number + 1000, 1000 FROM numbers(1000);

CREATE TABLE t_merge_sum AS t_merge_sum_1 ENGINE = Merge(currentDatabase(), '^t_merge_sum_[12]$');

SELECT 'explain_merge_sum_on';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, value
    FROM t_merge_sum FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1, optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 5%';

SELECT 'explain_merge_sum_off';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, value
    FROM t_merge_sum FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 0
)
WHERE explain LIKE '%Description: limit%';

SELECT 'correctness_sum';
SELECT
    arraySort((SELECT groupArray((key, value)) FROM (SELECT key, value FROM t_merge_sum FINAL ORDER BY key LIMIT 5 SETTINGS optimize_final_limit_pushdown = 1)))
  = arraySort((SELECT groupArray((key, value)) FROM (SELECT key, value FROM t_merge_sum FINAL ORDER BY key LIMIT 5 SETTINGS optimize_final_limit_pushdown = 0)));

-- ReplacingMergeTree cannot early-terminate its FINAL merge; the pushed-down limit must not corrupt it.
DROP TABLE IF EXISTS t_merge_repl_1;
DROP TABLE IF EXISTS t_merge_repl_2;
DROP TABLE IF EXISTS t_merge_repl;

CREATE TABLE t_merge_repl_1 (key UInt64, version UInt64, value String) ENGINE = ReplacingMergeTree(version) ORDER BY key;
CREATE TABLE t_merge_repl_2 (key UInt64, version UInt64, value String) ENGINE = ReplacingMergeTree(version) ORDER BY key;

INSERT INTO t_merge_repl_1 SELECT number, 1, 'old' FROM numbers(100);
INSERT INTO t_merge_repl_1 SELECT number, 2, 'new' FROM numbers(100);
INSERT INTO t_merge_repl_2 SELECT number + 100, 1, 'old' FROM numbers(100);
INSERT INTO t_merge_repl_2 SELECT number + 100, 2, 'new' FROM numbers(100);

CREATE TABLE t_merge_repl AS t_merge_repl_1 ENGINE = Merge(currentDatabase(), '^t_merge_repl_[12]$');

SELECT 'correctness_replacing';
SELECT
    arraySort((SELECT groupArray((key, value)) FROM (SELECT key, value FROM t_merge_repl FINAL ORDER BY key LIMIT 5 SETTINGS optimize_final_limit_pushdown = 1)))
  = arraySort((SELECT groupArray((key, value)) FROM (SELECT key, value FROM t_merge_repl FINAL ORDER BY key LIMIT 5 SETTINGS optimize_final_limit_pushdown = 0)));

DROP TABLE t_merge_agg;
DROP TABLE t_merge_agg_1;
DROP TABLE t_merge_agg_2;
DROP TABLE t_merge_sum;
DROP TABLE t_merge_sum_1;
DROP TABLE t_merge_sum_2;
DROP TABLE t_merge_repl;
DROP TABLE t_merge_repl_1;
DROP TABLE t_merge_repl_2;

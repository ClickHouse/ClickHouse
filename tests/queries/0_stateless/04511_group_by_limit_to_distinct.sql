-- The GroupByLimitToDistinct pass rewrites `SELECT key_expr FROM t GROUP BY key_expr LIMIT n`
-- (no aggregate functions, no HAVING / ORDER BY / QUALIFY / LIMIT BY / window clauses, no
-- GROUP BY modifiers, projection == GROUP BY keys) into `SELECT DISTINCT key_expr FROM t LIMIT n`,
-- which stops reading the input once `n` distinct groups are produced and streams the results.
-- https://github.com/ClickHouse/ClickHouse/issues/110047

SET enable_analyzer = 1;
-- The rewrite is opt-in: it takes the query off the aggregation path, which changes observable
-- query statistics (`rows_before_limit_at_least`, the rows read by a distributed query) and
-- supersedes `optimize_trivial_group_by_limit_query`. Enable it explicitly for this test.
SET optimize_group_by_limit_to_distinct = 1;

DROP TABLE IF EXISTS t_group_by_limit_distinct;

CREATE TABLE t_group_by_limit_distinct (v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_group_by_limit_distinct SELECT number % 10 FROM numbers(1000000);

-- The rewrite fires: the plan has a Distinct step and no Aggregating step.
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 5
);

-- Disabled by the setting: the aggregation is kept.
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 5 SETTINGS optimize_group_by_limit_to_distinct = 0
);

-- GROUP BY ALL resolves to the projection columns and is rewritten as well.
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY ALL LIMIT 5
);

-- The keys may be arbitrary expressions and listed in a different order than the projection.
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v % 2, v FROM t_group_by_limit_distinct GROUP BY v, v % 2 LIMIT 5
);

-- The rewrite must NOT fire when the query is not a pure deduplication:

-- ... an aggregate function in the projection;
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v, count() FROM t_group_by_limit_distinct GROUP BY v LIMIT 5
);

-- ... HAVING;
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v HAVING v > 0 LIMIT 5
);

-- ... ORDER BY (the query is no longer free to return arbitrary `n` groups cheaply,
-- keep it out of scope of this rewrite);
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v ORDER BY v LIMIT 5
);

-- ... LIMIT BY;
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 1 BY v LIMIT 5
);

-- ... GROUP BY modifiers: WITH TOTALS, WITH ROLLUP, WITH CUBE, GROUPING SETS;
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v WITH TOTALS LIMIT 5
);
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v WITH ROLLUP LIMIT 5
);
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v WITH CUBE LIMIT 5
);
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY GROUPING SETS ((v), ()) LIMIT 5
);

-- ... LIMIT + OFFSET above optimize_group_by_limit_to_distinct_max_limit (a large limit would
-- route high-cardinality data through the single-stream final distinct transform, which is
-- slower than parallel aggregation and cannot spill to disk);
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 1001
);
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 1000 OFFSET 1
);

-- (exactly at the threshold the rewrite still fires; OFFSET counts towards it because
-- DISTINCT has to produce LIMIT + OFFSET distinct rows before the read can stop)
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 995 OFFSET 5
);

-- (the threshold is adjustable)
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 5 SETTINGS optimize_group_by_limit_to_distinct_max_limit = 4
);

-- (setting the threshold to 0 effectively disables the rewrite)
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 5 SETTINGS optimize_group_by_limit_to_distinct_max_limit = 0
);

-- ... no LIMIT (the rewrite would buy nothing);
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v
);

-- ... the projection is narrower than the keys (`GROUP BY a, b` returns a row per (a, b)
-- pair, so projecting only `a` may legitimately produce duplicate values of `a`);
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v, v % 2 LIMIT 5
);

-- The rewrite fires under DISTINCT/GROUP BY size limits (commonly set as global sanity
-- limits, e.g. in the CI test configs) without changing which limits apply to the query:
-- the DISTINCT limits are cleared for the rewritten query (the user did not write DISTINCT,
-- and the distinct set is bounded by LIMIT + OFFSET rows anyway), and max_rows_to_group_by
-- stops applying together with the aggregation it guards.
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 5 SETTINGS max_rows_in_distinct = 100
);
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 5 SETTINGS max_rows_to_group_by = 1000000
);

-- Behavioral check: a distinct-rows limit smaller than the number of groups does not apply
-- to the rewritten query (the original aggregation would not have been subject to it either).
SELECT count() FROM (
    SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 5 SETTINGS max_rows_in_distinct = 3
);

-- ... group_by_use_nulls is enabled (conservative, same as other GROUP BY passes).
SELECT countIf(explain LIKE '%Distinct%') > 0, countIf(explain LIKE '%Aggregating%') > 0 FROM (
    EXPLAIN PLAN SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 5 SETTINGS group_by_use_nulls = 1
);

-- The rewrite lets the read stop early: 5 distinct values are found within the first block of
-- the 1M-row table. `max_rows_to_read` cannot demonstrate this -- for MergeTree it is validated
-- upfront against the rows of the selected parts, before any reading starts, so it fires however
-- early the query would have terminated. Compare the rows actually read instead.
--
-- `enable_parallel_replicas = 0` is pinned for the same reason as in
-- 04229_trivial_group_by_limit_profile_events: the pass runs in the analyzer on the initiator and
-- mutates the query's local context, and that mutation does not propagate to remote replicas.
SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 5 FORMAT Null
SETTINGS optimize_group_by_limit_to_distinct = 1, max_threads = 1,
    enable_parallel_replicas = 0, log_comment = '04511_on';

-- Without the rewrite (and with the max_rows_to_group_by optimization disabled as well,
-- to pin down the pass under test) the aggregation reads the whole table.
SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 5 FORMAT Null
SETTINGS optimize_group_by_limit_to_distinct = 0, optimize_trivial_group_by_limit_query = 0,
    max_threads = 1, enable_parallel_replicas = 0, log_comment = '04511_off';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    read_rows < 1000000 AS stopped_early
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('04511_on', '04511_off')
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY log_comment;

-- The result is the same set of groups (LIMIT larger than the number of groups).
SELECT v FROM (SELECT v FROM t_group_by_limit_distinct GROUP BY v LIMIT 100) ORDER BY v;

-- Semantic check for the narrower-projection case: 6 (a, b) groups, not 3 distinct `a`.
SELECT count() FROM (
    SELECT a FROM (SELECT number % 3 AS a, number % 2 AS b FROM numbers(100)) GROUP BY a, b LIMIT 100
);

DROP TABLE t_group_by_limit_distinct;

-- Tags: long
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_filter_push_down_inferred_only_for_pruning = 1;

DROP TABLE IF EXISTS rr_src;
DROP TABLE IF EXISTS rr_plain;

CREATE TABLE rr_src   (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
-- k is not covered by any index: an inferred copy of a k-condition cannot prune here
CREATE TABLE rr_plain (k UInt64, payload String) ENGINE = MergeTree ORDER BY payload;

INSERT INTO rr_src   SELECT number, toString(number) FROM numbers(1000000);
INSERT INTO rr_plain SELECT number, toString(number) FROM numbers(1000000);

-- Runtime join filters trigger the post-runtime-filter push-down rerun in optimizeTree.
-- The rerun must run with the real optimizer extra settings: with the inferred-copy gate
-- enabled, the non-pruning inferred copy must stay absent (1 = original side only).
SELECT 'rerun respects gate',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM rr_src AS o
    INNER JOIN rr_plain AS l ON o.k = l.k
    WHERE o.k = 12345
    SETTINGS enable_join_runtime_filters = 1
);

SELECT 'rerun correctness',
       (SELECT count() FROM rr_src AS o INNER JOIN rr_plain AS l ON o.k = l.k
        WHERE o.k = 12345 SETTINGS enable_join_runtime_filters = 1)
     - (SELECT count() FROM rr_src AS o INNER JOIN rr_plain AS l ON o.k = l.k
        WHERE o.k = 12345 SETTINGS enable_join_runtime_filters = 0);

DROP TABLE rr_src;
DROP TABLE rr_plain;

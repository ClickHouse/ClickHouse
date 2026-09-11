-- The push-down rerun that follows adding join runtime filters must use the real optimizer
-- extra settings, so merged step descriptions honour `query_plan_max_step_description_length`
-- instead of being dropped.
SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;

DROP TABLE IF EXISTS rr_left;
DROP TABLE IF EXISTS rr_right;

CREATE TABLE rr_left (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE rr_right (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO rr_left SELECT number FROM numbers(100000);
INSERT INTO rr_right SELECT number FROM numbers(10);

SELECT 'default length';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (
    EXPLAIN PLAN SELECT count() FROM rr_left AS l INNER JOIN rr_right AS r ON l.k = r.k
) WHERE explain LIKE '%Apply runtime join filter%';

SELECT 'truncated length';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (
    EXPLAIN PLAN SELECT count() FROM rr_left AS l INNER JOIN rr_right AS r ON l.k = r.k
    SETTINGS query_plan_max_step_description_length = 12
) WHERE explain LIKE '%Apply runt%';

DROP TABLE rr_left;
DROP TABLE rr_right;

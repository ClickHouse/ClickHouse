-- A common-type conversion can change a primary-key order: `Enum` is ordered by its numeric value,
-- while `String` is ordered lexically. Do not select `sorted_merge` based on the uncast key names;
-- fall through to `hash`, which also retains the runtime filter.

DROP TABLE IF EXISTS smj_cast_left;
DROP TABLE IF EXISTS smj_cast_right;

CREATE TABLE smj_cast_left (id Enum8('ten' = 10, 'two' = 2), value UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE smj_cast_right (id String, value UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO smj_cast_left VALUES ('ten', 1), ('two', 2);
INSERT INTO smj_cast_right VALUES ('10', 1), ('2', 2);

SET enable_analyzer = 1;
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_swap_table = 0, enable_parallel_replicas = 0;

SELECT 'cast_falls_through', countIf(explain LIKE '%MergeJoinTransform%') = 0
FROM
(
    EXPLAIN PIPELINE
    SELECT l.value
    FROM smj_cast_left AS l
    INNER JOIN smj_cast_right AS r ON l.id = r.id
    SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4
);

DROP TABLE smj_cast_left;
DROP TABLE smj_cast_right;

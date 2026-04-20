-- Tags: no-random-settings, no-random-merge-tree-settings

-- { echo ON }
SET enable_analyzer = 1;
SET min_table_rows_to_use_projection_index = 0;
SET max_projection_rows_to_use_projection_index = 1000000000;
DROP TABLE IF EXISTS t_point;
CREATE TABLE t_point
(
    id UInt64,
    trace_id FixedString(16),
    payload String,
    PROJECTION by_trace_id (SELECT _part_offset ORDER BY trace_id)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 32;
-- Multiple parts so narrowing runs per part.
INSERT INTO t_point
    SELECT number, unhex(hex(sipHash128(toString(number)))), 'x'
    FROM numbers(50000);
INSERT INTO t_point
    SELECT 50000 + number, unhex(hex(sipHash128(toString(50000 + number)))), 'x'
    FROM numbers(50000);
INSERT INTO t_point
    SELECT 100000 + number, unhex(hex(sipHash128(toString(100000 + number)))), 'x'
    FROM numbers(50000);
CREATE TEMPORARY TABLE _tid AS SELECT trace_id AS v FROM t_point LIMIT 1;
-- Correctness: same results with and without narrowing.
SELECT 'off', count()
FROM t_point WHERE trace_id = (SELECT v FROM _tid)
SETTINGS optimize_use_projection_filtering = 1, projection_index_narrow_marks = 0;
SELECT 'on', count()
FROM t_point WHERE trace_id = (SELECT v FROM _tid)
SETTINGS optimize_use_projection_filtering = 1, projection_index_narrow_marks = 1;
-- All columns match.
SELECT 'match', d.id = e.id
FROM
    (SELECT * FROM t_point WHERE trace_id = (SELECT v FROM _tid)
     SETTINGS optimize_use_projection_filtering = 1, projection_index_narrow_marks = 0) d
JOIN
    (SELECT * FROM t_point WHERE trace_id = (SELECT v FROM _tid)
     SETTINGS optimize_use_projection_filtering = 1, projection_index_narrow_marks = 1) e
USING (trace_id);
-- Non-existent key returns empty.
SELECT 'missing', count()
FROM t_point WHERE trace_id = unhex('00000000000000000000000000000000')
SETTINGS optimize_use_projection_filtering = 1, projection_index_narrow_marks = 1;
DROP TABLE t_point;

-- The queries of projections are analyzed and calculated with the Analyzer. Check that the columns
-- written into a projection part are exactly the columns of the projection metadata, and that the
-- optimizer still matches the projections it describes.

DROP TABLE IF EXISTS t_projection_analyzer;

CREATE TABLE t_projection_analyzer
(
    a UInt64,
    b String,
    d Date,
    tup Tuple(x UInt64, y String),
    al UInt64 ALIAS a * 2,
    PROJECTION p_agg (SELECT b, -a, count(), sum(a), sumIf(a, a > 3), uniqState(a) GROUP BY b, -a),
    PROJECTION p_alias (SELECT al, count() GROUP BY al),
    PROJECTION p_expr (SELECT CAST(a, 'String'), toYYYYMM(d), count() GROUP BY CAST(a, 'String'), toYYYYMM(d)),
    PROJECTION p_tuple (SELECT (a, b), count() GROUP BY (a, b)),
    PROJECTION p_normal (SELECT a, b ORDER BY b),
    PROJECTION p_filtered (SELECT b, sum(a) WHERE a > 3 GROUP BY b)
)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 4, materialize_projections_on_insert = 1, lightweight_mutation_projection_mode = 'rebuild';

INSERT INTO t_projection_analyzer SELECT number % 7, toString(number % 3), '2020-01-01', (number, toString(number)) FROM numbers(20);
INSERT INTO t_projection_analyzer SELECT number % 7, toString(number % 3), '2020-02-01', (number, toString(number)) FROM numbers(13);

SELECT 'columns of the projection parts written on insert';
SELECT name, arraySort(groupArray(DISTINCT column)) FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_analyzer' AND active
GROUP BY name ORDER BY name;

OPTIMIZE TABLE t_projection_analyzer FINAL;

SELECT 'columns of the projection parts written on merge';
SELECT name, arraySort(groupArray(DISTINCT column)) FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_analyzer' AND active
GROUP BY name ORDER BY name;

SET force_optimize_projection = 1;

SELECT 'p_agg';
SELECT b, -a, count(), sum(a), sumIf(a, a > 3) FROM t_projection_analyzer GROUP BY b, -a ORDER BY 1, 2;
SELECT 'p_alias';
SELECT al, count() FROM t_projection_analyzer GROUP BY al ORDER BY 1;
SELECT 'p_expr';
SELECT CAST(a, 'String'), toYYYYMM(d), count() FROM t_projection_analyzer GROUP BY CAST(a, 'String'), toYYYYMM(d) ORDER BY 1, 2;
SELECT 'p_tuple';
SELECT (a, b), count() FROM t_projection_analyzer GROUP BY (a, b) ORDER BY 1;

SET force_optimize_projection = 0;

SELECT 'after a lightweight delete';
DELETE FROM t_projection_analyzer WHERE a = 3;

SELECT count(), sum(a) FROM t_projection_analyzer;
SELECT b, sum(a) FROM t_projection_analyzer GROUP BY b ORDER BY b;
SELECT b, sum(a) FROM t_projection_analyzer WHERE a > 3 GROUP BY b ORDER BY b;

SELECT 'columns of the projection parts after the mutation';
SELECT name, arraySort(groupArray(DISTINCT column)) FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_analyzer' AND active
GROUP BY name ORDER BY name;

DROP TABLE t_projection_analyzer;

-- A normal projection which stores the offset of the row in the parent part.

DROP TABLE IF EXISTS t_projection_analyzer_offset;

CREATE TABLE t_projection_analyzer_offset (a UInt64, b String, c String)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4, materialize_projections_on_insert = 1;

ALTER TABLE t_projection_analyzer_offset ADD PROJECTION p (SELECT b, _part_offset ORDER BY b);

INSERT INTO t_projection_analyzer_offset SELECT number, toString(number % 5), toString(number) FROM numbers(20);

SELECT 'columns of a normal projection with the parent part offset';
SELECT name, arraySort(groupArray(DISTINCT column)) FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_analyzer_offset' AND active
GROUP BY name ORDER BY name;

SELECT c FROM t_projection_analyzer_offset WHERE b = '3' ORDER BY c;

DROP TABLE t_projection_analyzer_offset;

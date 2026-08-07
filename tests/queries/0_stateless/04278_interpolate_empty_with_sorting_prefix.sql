-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105997
-- INTERPOLATE () (empty) with sorting prefix threw INVALID_WITH_FILL_EXPRESSION

SET use_with_fill_by_sorting_prefix = 1;

CREATE TABLE t_interpolate_empty_prefix (a UInt8, h DateTime, s String) ENGINE = Memory;
INSERT INTO t_interpolate_empty_prefix VALUES
    (1, '2024-01-01 00:00:00', 'v1'),
    (1, '2024-01-01 02:00:00', 'v2'),
    (2, '2024-01-01 00:00:00', 'v3'),
    (2, '2024-01-01 01:00:00', 'v4');

SELECT a, h, s
FROM t_interpolate_empty_prefix
ORDER BY a, h ASC
    WITH FILL STEP INTERVAL 1 HOUR
    INTERPOLATE ();

DROP TABLE t_interpolate_empty_prefix;

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103474
-- NOT_FOUND_COLUMN_IN_BLOCK when combining WITH FILL INTERPOLATE and LIMIT N BY

SET enable_analyzer = 1;

CREATE TABLE t_interpolate_limit_by (a UInt8, h DateTime, s String) ENGINE = Memory;
INSERT INTO t_interpolate_limit_by VALUES
    (1, '2024-01-01 00:00:00', 'v1'),
    (1, '2024-01-01 02:00:00', 'v2'),
    (2, '2024-01-01 00:00:00', 'v3'),
    (2, '2024-01-01 01:00:00', 'v4');

SELECT a, h, s
FROM t_interpolate_limit_by
ORDER BY a, h ASC
    WITH FILL STEP INTERVAL 1 HOUR
    INTERPOLATE (s)
LIMIT 1 BY a, h;

SELECT a, h, s
FROM t_interpolate_limit_by
ORDER BY a, h ASC
    WITH FILL STEP INTERVAL 1 HOUR
    INTERPOLATE ()
LIMIT 1 BY a, h;

SELECT a, h, s
FROM t_interpolate_limit_by
ORDER BY a, h ASC
    WITH FILL STEP INTERVAL 1 HOUR
    INTERPOLATE (s AS upper(s))
LIMIT 1 BY a, h;

DROP TABLE t_interpolate_limit_by;

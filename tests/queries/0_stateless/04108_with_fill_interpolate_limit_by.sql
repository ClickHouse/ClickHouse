SELECT number AS k, 'hello' AS s
FROM numbers(5)
WHERE number % 2 = 0
ORDER BY k WITH FILL FROM 0 TO 4
INTERPOLATE (s AS concat(s, '!'))
LIMIT 1 BY k;

DROP TABLE IF EXISTS test_interp_lb_04108;
CREATE TABLE test_interp_lb_04108 (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO test_interp_lb_04108 VALUES (0, 'a'), (2, 'c'), (4, 'e');

SELECT k, v
FROM test_interp_lb_04108
ORDER BY k WITH FILL FROM 0 TO 4
INTERPOLATE (v AS concat(v, '!'))
LIMIT 1 BY k;

DROP TABLE test_interp_lb_04108;

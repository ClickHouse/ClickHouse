-- Tags: no-random-settings

SET enable_analyzer = 1;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS t_join_to_in_all_left;
DROP TABLE IF EXISTS t_join_to_in_all_right;

CREATE TABLE t_join_to_in_all_left (id UInt8) ENGINE = Memory;
CREATE TABLE t_join_to_in_all_right (id UInt8) ENGINE = Memory;

INSERT INTO t_join_to_in_all_left VALUES (1);
INSERT INTO t_join_to_in_all_right VALUES (1), (1);

SELECT l.id
FROM t_join_to_in_all_left AS l
ALL INNER JOIN t_join_to_in_all_right AS r ON l.id = r.id
ORDER BY l.id
SETTINGS query_plan_convert_join_to_in = 1;

DROP TABLE t_join_to_in_all_left;
DROP TABLE t_join_to_in_all_right;

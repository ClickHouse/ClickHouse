-- `LATERAL` is only treated as the `JOIN LATERAL` keyword when a parenthesized subquery follows it,
-- so a table named `lateral` on the right side of a join still parses, regardless of the setting.

DROP TABLE IF EXISTS t_lateral_left;
DROP TABLE IF EXISTS lateral;

CREATE TABLE t_lateral_left (id UInt32) ENGINE = Memory;
CREATE TABLE lateral (id UInt32, v String) ENGINE = Memory;

INSERT INTO t_lateral_left VALUES (1), (2), (3);
INSERT INTO lateral VALUES (1, 'a'), (3, 'c');

SET allow_experimental_lateral_join = 0;

SELECT t.id, v FROM t_lateral_left AS t JOIN lateral ON t.id = lateral.id ORDER BY t.id;
SELECT t.id, l.v FROM t_lateral_left AS t LEFT JOIN lateral AS l ON t.id = l.id ORDER BY t.id;
SELECT t.id, v FROM t_lateral_left AS t JOIN lateral USING (id) ORDER BY t.id;
SELECT t.id, v FROM t_lateral_left AS t, lateral WHERE t.id = lateral.id ORDER BY t.id;
SELECT count() FROM t_lateral_left AS t CROSS JOIN lateral;

SET allow_experimental_lateral_join = 1;

SELECT t.id, v FROM t_lateral_left AS t JOIN lateral ON t.id = lateral.id ORDER BY t.id;
SELECT t.id, l.v FROM t_lateral_left AS t LEFT JOIN LATERAL (SELECT v FROM lateral WHERE lateral.id = t.id) AS l ON true ORDER BY t.id;

DROP TABLE t_lateral_left;
DROP TABLE lateral;

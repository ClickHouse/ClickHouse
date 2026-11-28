-- A JOIN USING key resolved from a projection alias over an ALIAS column, when the columns of the left table
-- were already looked up before that key.

DROP TABLE IF EXISTS t_alias_using_l;
DROP TABLE IF EXISTS t_alias_using_x;
DROP TABLE IF EXISTS t_alias_using_r;

CREATE TABLE t_alias_using_l (id UInt64, al UInt64 ALIAS id * 10, al2 UInt64 ALIAS al + 1) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_alias_using_x (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_alias_using_r (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_alias_using_l VALUES (1), (2);
INSERT INTO t_alias_using_x VALUES (1), (2);
INSERT INTO t_alias_using_r VALUES (1, 10), (2, 20), (3, 30), (1, 11), (2, 21);

SET analyzer_compatibility_join_using_top_level_identifier = 1;

SELECT id, al AS k FROM t_alias_using_l JOIN t_alias_using_r USING (k, id) ORDER BY id;
SELECT id, al AS k FROM t_alias_using_l JOIN t_alias_using_r USING (id, k) ORDER BY id;
-- The left side reads the left table in JOIN ON.
SELECT l.id, al AS k FROM t_alias_using_l AS l JOIN t_alias_using_x AS x ON l.id = x.id JOIN t_alias_using_r USING (k) ORDER BY l.id;
-- An ALIAS column that reads another ALIAS column, which the query uses too.
SELECT id, al2 AS k, al FROM t_alias_using_l JOIN t_alias_using_r USING (id, k) ORDER BY id;

DROP TABLE t_alias_using_l;
DROP TABLE t_alias_using_x;
DROP TABLE t_alias_using_r;

DROP TABLE IF EXISTS t_create_as_tuple;

CREATE TABLE t_create_as_tuple ENGINE = MergeTree()
ORDER BY number AS
SELECT number, [('string',number)] AS array FROM numbers(3);

SELECT * FROM t_create_as_tuple ORDER BY number;

DETACH TABLE t_create_as_tuple;
ATTACH TABLE t_create_as_tuple;

SELECT * FROM t_create_as_tuple ORDER BY number;

DROP TABLE t_create_as_tuple;

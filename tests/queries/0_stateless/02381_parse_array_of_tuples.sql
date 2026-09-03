DROP TABLE IF EXISTS t_parse_tuples;

CREATE TABLE t_parse_tuples
(
    id UInt32,
    arr Array(Array(Tuple(c1 Int32, c2 UInt8)))
)
ENGINE = Memory;

INSERT INTO t_parse_tuples VALUES (1, [[]]), (2, [[(500, -10)]]), (3, [[(500, '10')]]);

SELECT * FROM t_parse_tuples ORDER BY id;

DROP TABLE IF EXISTS t_parse_tuples;

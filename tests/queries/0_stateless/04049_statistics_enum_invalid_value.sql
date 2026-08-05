DROP TABLE IF EXISTS t_enum_stats;

CREATE TABLE t_enum_stats
(
    a Int64,
    x Enum('hello' = 1, 'world' = 2)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax';

INSERT INTO t_enum_stats VALUES (1, 'hello');

SELECT * FROM t_enum_stats WHERE a = 1 AND x = '';

DROP TABLE t_enum_stats;

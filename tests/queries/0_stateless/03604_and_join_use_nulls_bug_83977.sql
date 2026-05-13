CREATE TABLE AA ( `key` String, `value` Int64 ) ENGINE = MergeTree ORDER BY (key);
INSERT INTO AA VALUES ('a', 5), ('b', 15);

CREATE TABLE B ( `key` String, `flag` Bool ) ENGINE = MergeTree ORDER BY (key);
INSERT INTO B VALUES ('a', 1), ('c', 0);

CREATE TABLE C ( `key` String ) ENGINE = MergeTree ORDER BY (key);
INSERT INTO C VALUES ('a'), ('d');

SELECT
    `flag` AND value <= 10
FROM (
    SELECT * FROM AA
    LEFT JOIN B AS `t0` ON AA.`key` = `t0`.`key`
    LEFT JOIN C AS `t1` ON AA.`key` = `t1`.`key`
)
ORDER BY ALL
SETTINGS join_use_nulls = 1, enable_analyzer = 1
;

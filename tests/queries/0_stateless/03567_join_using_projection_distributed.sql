DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 ( `key` String, `attr` UInt32 ) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2 ( `key` String, `attr` UInt32 ) ENGINE = MergeTree ORDER BY key;

INSERT INTO t1 VALUES ('a', 42), ('b', 43), ('c', 44);
INSERT INTO t2 VALUES ('AA', 111), ('AA', 222), ('other', 333);

SELECT
    CASE
              WHEN key = 'a' THEN 'AA'
              WHEN key = 'b' THEN 'BB'
              ELSE 'other'
    END AS key1,
    *
FROM remote('127.0.0.{2,3}', currentDatabase(), t1) t1
INNER JOIN
(
    SELECT
        key AS key1,
        attr
    FROM t2
) AS a USING (key1)
ORDER BY a.attr
SETTINGS enable_analyzer = 1, analyzer_compatibility_join_using_top_level_identifier=1;


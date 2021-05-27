DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1
(
    `n` Int8
)
ENGINE = Memory
COMMENT 'this is a temtorary table';

CREATE TABLE t2
(
    `n` Int8
)
ENGINE = MergeTree
ORDER BY n
COMMENT 'this is a MergeTree table';

CREATE TABLE t3
(
    `n` Int8
)
ENGINE = Log
COMMENT 'this is a Log table';

SELECT
    name,
    comment
FROM system.tables
WHERE name IN ('t1', 't2', 't3') AND database = currentDatabase() order by name;

SHOW CREATE TABLE t1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS d;

CREATE TABLE m
(
    `v` UInt8
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY v;

CREATE TABLE d
(
    `v` UInt16
)
ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), m, rand());

INSERT INTO m VALUES (123);
SELECT * FROM d;


DROP TABLE m;
DROP TABLE d;


CREATE TABLE m
(
    `v` Enum8('a' = 1, 'b' = 2)
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY v;

CREATE TABLE d
(
    `v` Enum8('a' = 1)
)
ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), m, rand());

INSERT INTO m VALUES ('a');
SELECT * FROM d;

SELECT '---';

INSERT INTO m VALUES ('b');
SELECT toString(v) FROM (SELECT v FROM d ORDER BY v) FORMAT Null; -- { serverError BAD_ARGUMENTS }


DROP TABLE m;
DROP TABLE d;

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
-- The declared `Enum8('a' = 1)` does not contain every value of the shard's
-- `Enum8('a' = 1, 'b' = 2)`, so converting to it neither preserves the order nor is defined at all.
-- `StorageDistributed::getQueryProcessingStage` refuses the ORDER BY before the shard query runs,
-- which used to fail later on the conversion of the fetched 'b' with `UNKNOWN_ELEMENT_OF_ENUM`.
SELECT toString(v) FROM (SELECT v FROM d ORDER BY v) FORMAT Null; -- { serverError INCOMPATIBLE_COLUMNS }


DROP TABLE m;
DROP TABLE d;

DROP TABLE IF EXISTS test;
CREATE TABLE test
(
  id   UInt32,
  code LowCardinality(FixedString(2)) DEFAULT '--'
) ENGINE = MergeTree() PARTITION BY id ORDER BY id;

INSERT INTO test FORMAT CSV 1,RU
INSERT INTO test FORMAT CSV 1,

SELECT * FROM test ORDER BY code;
OPTIMIZE TABLE test;
SELECT * FROM test ORDER BY code;

DROP TABLE test;

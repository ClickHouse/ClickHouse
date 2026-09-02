DROP TABLE IF EXISTS test;

CREATE TABLE test
(
  EventDate Date
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY toMonday(EventDate);

INSERT INTO test VALUES(toDate('2023-10-09'));

ALTER TABLE test DROP PARTITION ('2023-10-09');

SELECT count() FROM test;

INSERT INTO test VALUES(toDate('2023-10-09'));

ALTER TABLE test DROP PARTITION (('2023-10-09'));

SELECT count() FROM test;

INSERT INTO test VALUES(toDate('2023-10-09'));

ALTER TABLE test DROP PARTITION '2023-10-09';

SELECT count() FROM test;

INSERT INTO test VALUES(toDate('2023-10-09'));

SET param_partition='2023-10-09';

ALTER TABLE test DROP PARTITION {partition:String};

SELECT count() FROM test;

INSERT INTO test VALUES(toDate('2023-10-09'));

ALTER TABLE test DROP PARTITION tuple(toMonday({partition:Date}));

SELECT count() FROM test;

INSERT INTO test VALUES(toDate('2023-10-09'));

-- for some reason only tuples are allowed as non-string arguments
ALTER TABLE test DROP PARTITION toMonday({partition:String}); --{clientError SYNTAX_ERROR}

set param_partition_id = '20231009';

ALTER TABLE test DROP PARTITION ID {partition_id:String};

SELECT count() FROM test;

INSERT INTO test VALUES(toDate('2023-10-09'));

ALTER TABLE test DROP PARTITION {partition:Date};
SELECT count() FROM test;

DROP TABLE IF EXISTS test;

DROP TABLE IF EXISTS test2;

CREATE TABLE test2
(
  a UInt32,
  b Int64
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY (a * b, b * b);

INSERT INTO test2 VALUES(1, 2);

ALTER TABLE test2 DROP PARTITION tuple(2, 4);

SELECT count() FROM test2;

INSERT INTO test2 VALUES(1, 2);

ALTER TABLE test2 DROP PARTITION (2, 4);

SELECT count() FROM test2;

INSERT INTO test2 VALUES(1, 2);

SET param_first='2';
SET param_second='4';

ALTER TABLE test2 DROP PARTITION tuple({first:UInt32},{second:Int64});

SELECT count() FROM test2;

DROP TABLE IF EXISTS test2;
DROP TABLE IF EXISTS test3;

CREATE TABLE test3
(
  a UInt32,
  b Int64
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY a;

INSERT INTO test3 VALUES(1, 2);

SET param_simple='1';

ALTER TABLE test3 DROP PARTITION {simple:String};

SELECT count() FROM test3;

DROP TABLE IF EXISTS test3;

DROP TABLE IF EXISTS test4;

CREATE TABLE test4 (EventDate Date) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY EventDate;

INSERT INTO test4 VALUES(toDate('2023-10-09'));

SET param_partition='2023-10-09';

ALTER TABLE test4 ON CLUSTER 'test_shard_localhost' DROP PARTITION {partition:String} FORMAT Null;

SELECT count() FROM test4;

DROP TABLE IF EXISTS test4;

DROP TABLE IF EXISTS test5;

CREATE TABLE test5
(
  a UInt32,
  b Int64
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY (a, b);

INSERT INTO test5 VALUES(1, 2);

SET param_f='1';
SET param_s='2';

ALTER TABLE test5 DROP PARTITION ({f:UInt32}, 2);

SELECT count() FROM test5;

DROP TABLE IF EXISTS test5;

DROP TABLE IF EXISTS test6;

CREATE TABLE test6
(
  a UInt32,
  b Int64
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY (a, b);

INSERT INTO test6 VALUES(1, 2);

SET param_tuple=(1, 2);

ALTER TABLE test6 DROP PARTITION {tuple:Tuple(UInt32, Int64)};

SELECT count() FROM test6;

DROP TABLE IF EXISTS test6;

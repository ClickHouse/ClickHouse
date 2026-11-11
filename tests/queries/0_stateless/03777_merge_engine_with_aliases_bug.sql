SET enable_analyzer=1;

DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;

CREATE TABLE test1
(
     a Int32,
     b Int32,
     a_a Int32 ALIAS a
)
ENGINE = MergeTree() order by tuple();

CREATE TABLE test2
(
     a Int32,
     c Int32,
     a_a Int16 ALIAS a
)
ENGINE = MergeTree() order by tuple();

INSERT INTO test1 SELECT 42, 43;
INSERT INTO test2 SELECT 44, 45;

DESC merge(currentDatabase(), '^test.*');
SELECT * FROM merge(currentDatabase(), '^test.*') order by all;
SELECT a, b, c, a_a FROM merge(currentDatabase(), '^test.*') order by all;

DROP TABLE test1;
DROP TABLE test2;


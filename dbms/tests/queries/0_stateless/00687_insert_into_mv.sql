DROP TABLE IF EXISTS test.test;
DROP TABLE IF EXISTS test.mv_bad;
DROP TABLE IF EXISTS test.mv_good;
DROP TABLE IF EXISTS test.mv_group;

CREATE TABLE test.test (x String) ENGINE = Null;

create MATERIALIZED VIEW test.mv_bad (x String)
ENGINE = MergeTree Partition by tuple() order by tuple()
AS SELECT DISTINCT x FROM test.test;

create MATERIALIZED VIEW test.mv_good (x String)
ENGINE = MergeTree Partition by tuple() order by tuple()
AS SELECT x FROM test.test;

create MATERIALIZED VIEW test.mv_group (x String)
ENGINE = MergeTree Partition by tuple() order by tuple()
AS SELECT x FROM test.test group by x;

insert into test.test values ('stest'), ('stest');

select * from test.mv_bad;
SELECT '---';
select * from test.mv_good;
SELECT '---';
select * from test.mv_group;

DROP TABLE test.mv_bad;
DROP TABLE test.mv_good;
DROP TABLE test.mv_group;
DROP TABLE test.test;

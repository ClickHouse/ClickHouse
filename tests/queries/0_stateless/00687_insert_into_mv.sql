DROP TABLE IF EXISTS test_00687;
DROP TABLE IF EXISTS mv_bad;
DROP TABLE IF EXISTS mv_good;
DROP TABLE IF EXISTS mv_group;

CREATE TABLE test_00687 (x String) ENGINE = Null;

create MATERIALIZED VIEW mv_bad (x String)
ENGINE = MergeTree Partition by tuple() order by tuple()
AS SELECT DISTINCT x FROM test_00687;

create MATERIALIZED VIEW mv_good (x String)
ENGINE = MergeTree Partition by tuple() order by tuple()
AS SELECT x FROM test_00687;

create MATERIALIZED VIEW mv_group (x String)
ENGINE = MergeTree Partition by tuple() order by tuple()
AS SELECT x FROM test_00687 group by x;

insert into test_00687 values ('stest'), ('stest');

select * from mv_bad;
SELECT '---';
select * from mv_good;
SELECT '---';
select * from mv_group;

DROP TABLE mv_bad;
DROP TABLE mv_good;
DROP TABLE mv_group;
DROP TABLE test_00687;

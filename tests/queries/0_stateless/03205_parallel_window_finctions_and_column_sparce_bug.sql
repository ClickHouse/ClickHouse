create table t(c Int32, d Bool) Engine=MergeTree order by c;
system stop merges t;

insert into t values (1, 0);
insert into t values (1, 0);
insert into t values (1, 1);
insert into t values (1, 0)(1, 1);

SELECT d, c, row_number() over (partition by d order by c) as c8 FROM t qualify c8=1 order by d settings max_threads=2;
SELECT '---';
SELECT d, c, row_number() over (partition by d order by c) as c8 FROM t order by d, c8 settings max_threads=2;

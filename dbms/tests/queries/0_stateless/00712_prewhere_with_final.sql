drop table if exists test.trepl;
create table test.trepl(d Date,a Int32, b Int32) engine = ReplacingMergeTree(d, (a,b), 8192);
insert into test.trepl values ('2018-09-19', 1, 1);
select b from test.trepl FINAL prewhere a < 1000;
drop table test.trepl;


drop table if exists test.versioned_collapsing;
create table test.versioned_collapsing(d Date, x UInt32, sign Int8, version UInt32) engine = VersionedCollapsingMergeTree(d, x, 8192, sign, version);
insert into test.versioned_collapsing values ('2018-09-19', 123, 1, 0);
select x from test.versioned_collapsing FINAL prewhere version < 1000;
drop table test.versioned_collapsing;

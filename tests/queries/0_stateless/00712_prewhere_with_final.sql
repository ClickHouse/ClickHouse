drop table if exists trepl;
set allow_deprecated_syntax_for_merge_tree=1;
create table trepl(d Date,a Int32, b Int32) engine = ReplacingMergeTree(d, (a,b), 8192);
insert into trepl values ('2018-09-19', 1, 1);
select b from trepl FINAL prewhere a < 1000;
drop table trepl;


drop table if exists versioned_collapsing;
create table versioned_collapsing(d Date, x UInt32, sign Int8, version UInt32) engine = VersionedCollapsingMergeTree(d, x, 8192, sign, version);
insert into versioned_collapsing values ('2018-09-19', 123, 1, 0);
select x from versioned_collapsing FINAL prewhere version < 1000;
drop table versioned_collapsing;

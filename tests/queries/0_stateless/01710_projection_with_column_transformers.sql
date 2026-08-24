drop table if exists foo;

create table foo(bar String, projection p (select * apply groupUniqArray(100))) engine MergeTree order by bar;

show create foo;

detach table foo;

attach table foo;

drop table foo;

-- Disable force_primary_key_reverse_order: SHOW CREATE output contains ORDER BY which changes with forced DESC
SET force_primary_key_reverse_order = 0;

drop table if exists foo;

create table foo(bar String, projection p (select * apply groupUniqArray(100))) engine MergeTree order by bar;

show create foo;

detach table foo;

attach table foo;

drop table foo;

drop table if exists prewhere_column_missing;

create table prewhere_column_missing (d Date default '2015-01-01', x UInt64) engine=MergeTree(d, x, 1);

insert into prewhere_column_missing (x) values (0);
select * from prewhere_column_missing;

alter table prewhere_column_missing add column arr Array(UInt64);
select * from prewhere_column_missing;

select *, arraySum(arr) as s from prewhere_column_missing;
select *, arraySum(arr) as s from prewhere_column_missing where s = 0;
select *, arraySum(arr) as s from prewhere_column_missing prewhere s = 0;

select *, length(arr) as l from prewhere_column_missing;
select *, length(arr) as l from prewhere_column_missing where l = 0;
select *, length(arr) as l from prewhere_column_missing prewhere l = 0;

alter table prewhere_column_missing add column hash_x UInt64 default intHash64(x);

select * from prewhere_column_missing;
select * from prewhere_column_missing where hash_x = intHash64(x);
select * from prewhere_column_missing prewhere hash_x = intHash64(x);
select * from prewhere_column_missing where hash_x = intHash64(x) and length(arr) = 0;
select * from prewhere_column_missing prewhere hash_x = intHash64(x) and length(arr) = 0;
select * from prewhere_column_missing where hash_x = intHash64(x) and length(arr) = 0 and arraySum(arr) = 0;
select * from prewhere_column_missing prewhere hash_x = intHash64(x) and length(arr) = 0 and arraySum(arr) = 0;

drop table prewhere_column_missing;

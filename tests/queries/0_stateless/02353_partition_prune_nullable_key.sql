drop table if exists n;

create table n(nc Nullable(int)) engine = MergeTree order by (tuple()) partition by (nc) settings allow_nullable_key = 1;

insert into n values (null);

select * from n where nc is null;

drop table n;

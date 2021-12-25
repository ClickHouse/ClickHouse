drop table if exists test_enum;
create table test_enum (c Nullable(Enum16('A' = 1, 'B' = 2))) engine Log;
insert into test_enum values (1), (NULL);
select * from test_enum;
drop table test_enum;

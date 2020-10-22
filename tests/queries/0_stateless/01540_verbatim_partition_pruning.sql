drop table if exists xy;

create table xy(x int, y int) engine MergeTree partition by intHash64(x) % 100 order by y settings index_granularity = 1;

insert into xy values (1, 2), (2, 3);

SET max_rows_to_read = 1;

select * from xy where intHash64(x) % 100 = intHash64(1) % 100;

drop table if exists xy;

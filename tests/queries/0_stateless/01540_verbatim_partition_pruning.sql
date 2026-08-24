drop table if exists xy;

create table xy(x int, y int) engine MergeTree partition by intHash64(x) % 2 order by y settings index_granularity = 1;

-- intHash64(0) % 2 = 0
-- intHash64(2) % 2 = 1
-- intHash64(8) % 2 = 0
-- intHash64(9) % 2 = 1
insert into xy values (0, 2), (2, 3), (8, 4), (9, 5);

-- Now we have two partitions: 0 and 1, each of which contains 2 values.
-- minmax index for the first partition is 0 <= x <= 8
-- minmax index for the second partition is 2 <= x <= 9

SET max_rows_to_read = 2;

select * from xy where intHash64(x) % 2 = intHash64(2) % 2;

-- Equality is another special operator that can be treated as an always monotonic indicator for deterministic functions.
-- minmax index is not enough.
select * from xy where x = 8;

drop table if exists xy;

-- Test if we provide enough columns to generate a partition value
drop table if exists xyz;
create table xyz(x int, y int, z int) engine MergeTree partition by if(toUInt8(x), y, z) order by x settings index_granularity = 1;
insert into xyz values (1, 2, 3);
select * from xyz where y = 2;
drop table if exists xyz;

-- Test if we obey strict rules when facing NOT contitions
drop table if exists test;
create table test(d Date, k Int64, s String) Engine=MergeTree partition by (toYYYYMM(d),k) order by (d, k);

insert into test values ('2020-01-01', 1, '');
insert into test values ('2020-01-02', 1, '');

select * from test where d != '2020-01-01';
drop table test;

-- Test if single value partition pruning works correctly for Date = String
drop table if exists myTable;
CREATE TABLE myTable (myDay Date, myOrder Int32, someData String) ENGINE = ReplacingMergeTree PARTITION BY floor(toYYYYMMDD(myDay), -1) ORDER BY (myOrder);
INSERT INTO myTable (myDay, myOrder) VALUES ('2021-01-01', 1);
INSERT INTO myTable (myDay, myOrder) VALUES ('2021-01-02', 2); -- This row should be returned
INSERT INTO myTable (myDay, myOrder) VALUES ('2021-01-03', 3);
SELECT * FROM myTable mt WHERE myDay = '2021-01-02';
drop table myTable;

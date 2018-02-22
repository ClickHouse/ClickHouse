set max_threads = 1;
drop table if exists test.enums;

create table test.enums (
    d Date default '2015-12-29', k default 0,
    e Enum8('world' = 2, 'hello' = 1), sign Enum8('minus' = -1, 'plus' = 1),
    letter Enum16('a' = 0, 'b' = 1, 'c' = 2, '*' = -256)
) engine = MergeTree(d, k, 1);

desc table test.enums;

-- insert default values
insert into test.enums (k) values (0);
select * from test.enums;

alter table test.enums modify column e Enum8('world' = 2, 'hello' = 1, '!' = 3);
desc table test.enums;

insert into test.enums (e, sign, letter) values ('!', 'plus', 'b');
select * from test.enums ORDER BY _part;

-- expand `e` and `sign` from Enum8 to Enum16 without changing values, change values of `letter` without changing type
alter table test.enums
    modify column e Enum16('world' = 2, 'hello' = 1, '!' = 3),
    modify column sign Enum16('minus' = -1, 'plus' = 1),
    modify column letter Enum16('a' = 0, 'b' = 1, 'c' = 2, 'no letter' = -256);
desc table test.enums;

select * from test.enums ORDER BY _part;

alter table test.enums
    modify column e Enum8('world' = 2, 'hello' = 1, '!' = 3),
    modify column sign Enum8('minus' = -1, 'plus' = 1);

desc table test.enums;

insert into test.enums (letter, e) values ('c', 'world');
select * from test.enums ORDER BY _part;

drop table test.enums;

create table test.enums (e Enum8('a' = 0, 'b' = 1, 'c' = 2, 'd' = 3)) engine = TinyLog;
insert into test.enums values ('d'), ('b'), ('a'), ('c'), ('a'), ('d');
select * from test.enums;

-- ORDER BY
select * from test.enums order by e;
select * from test.enums order by e desc;

-- GROUP BY
select count(), e from test.enums group by e;
select any(e) from test.enums;

-- IN
select * from test.enums where e in ('a', 'd');
select * from test.enums where e in (select e from test.enums);

-- DISTINCT
select distinct e from test.enums;

-- Comparison
select * from test.enums where e = e;
select * from test.enums where e = 'a' or e = 'd';
select * from test.enums where e != 'a';
select *, e < 'b' from test.enums;
select *, e > 'b' from test.enums;

-- Conversion
select toInt8(e), toInt16(e), toUInt64(e), toString(e), e from test.enums;

drop table if exists test.enums_copy;
create table test.enums_copy engine = TinyLog as select * from test.enums;
select * from test.enums_copy;

drop table test.enums_copy;
create table test.enums_copy engine = TinyLog as select * from remote('localhost', test, enums);
select * from remote('localhost', test, enums_copy);

drop table test.enums_copy;
drop table test.enums;

SELECT '----- START -----';
drop table if exists morton_numbers_02457;
create table morton_numbers_02457(
    n1 UInt32,
    n2 UInt32,
    n3 UInt16,
    n4 UInt16,
    n5 UInt8,
    n6 UInt8,
    n7 UInt8,
    n8 UInt8
)
    Engine=MergeTree()
    ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

SELECT '----- CONST -----';
select mortonEncode(1,2,3,4);
select mortonDecode(4, 2149);
select mortonEncode(65534, 65533);
select mortonDecode(2, 4294967286);
select mortonEncode(4294967286);
select mortonDecode(1, 4294967286);

SELECT '----- 256, 8 -----';
insert into morton_numbers_02457
select n1.number, n2.number, n3.number, n4.number, n5.number, n6.number, n7.number, n8.number
from numbers(256-4, 4) n1
    cross join numbers(256-4, 4) n2
    cross join numbers(256-4, 4) n3
    cross join numbers(256-4, 4) n4
    cross join numbers(256-4, 4) n5
    cross join numbers(256-4, 4) n6
    cross join numbers(256-4, 4) n7
    cross join numbers(256-4, 4) n8
;
drop table if exists morton_numbers_1_02457;
create table morton_numbers_1_02457(
    n1 UInt64,
    n2 UInt64,
    n3 UInt64,
    n4 UInt64,
    n5 UInt64,
    n6 UInt64,
    n7 UInt64,
    n8 UInt64
)
    Engine=MergeTree()
    ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into morton_numbers_1_02457
select untuple(mortonDecode(8, mortonEncode(n1, n2, n3, n4, n5, n6, n7, n8)))
from morton_numbers_02457;

(
    select * from morton_numbers_02457
    union distinct
    select * from morton_numbers_1_02457
)
except
(
    select * from morton_numbers_02457
    intersect
    select * from morton_numbers_1_02457
);
drop table if exists morton_numbers_1_02457;

SELECT '----- 65536, 4 -----';
insert into morton_numbers_02457
select n1.number, n2.number, n3.number, n4.number, 0, 0, 0, 0
from numbers(pow(2, 16)-8,8) n1
    cross join numbers(pow(2, 16)-8, 8) n2
    cross join numbers(pow(2, 16)-8, 8) n3
    cross join numbers(pow(2, 16)-8, 8) n4
;

create table morton_numbers_2_02457(
    n1 UInt64,
    n2 UInt64,
    n3 UInt64,
    n4 UInt64
)
    Engine=MergeTree()
    ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into morton_numbers_2_02457
select untuple(mortonDecode(4, mortonEncode(n1, n2, n3, n4)))
from morton_numbers_02457;

(
    select n1, n2, n3, n4 from morton_numbers_02457
    union distinct
    select n1, n2, n3, n4 from morton_numbers_2_02457
)
except
(
    select n1, n2, n3, n4 from morton_numbers_02457
    intersect
    select n1, n2, n3, n4 from morton_numbers_2_02457
);
drop table if exists morton_numbers_2_02457;

SELECT '----- 4294967296, 2 -----';
insert into morton_numbers_02457
select n1.number, n2.number, 0, 0, 0, 0, 0, 0
from numbers(pow(2, 32)-8,8) n1
    cross join numbers(pow(2, 32)-8, 8) n2
    cross join numbers(pow(2, 32)-8, 8) n3
    cross join numbers(pow(2, 32)-8, 8) n4
;

drop table if exists morton_numbers_3_02457;
create table morton_numbers_3_02457(
    n1 UInt64,
    n2 UInt64
)
    Engine=MergeTree()
    ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into morton_numbers_3_02457
select untuple(mortonDecode(2, mortonEncode(n1, n2)))
from morton_numbers_02457;

(
    select n1, n2 from morton_numbers_3_02457
    union distinct
    select n1, n2 from morton_numbers_3_02457
)
except
(
    select n1, n2 from morton_numbers_3_02457
    intersect
    select n1, n2 from morton_numbers_3_02457
);
drop table if exists morton_numbers_3_02457;

SELECT '----- END -----';
drop table if exists morton_numbers_02457;

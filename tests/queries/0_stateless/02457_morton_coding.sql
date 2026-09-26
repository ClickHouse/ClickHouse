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

-- Every code below stays inside the documented input domain (code <= 2^(ND*FieldBits)-1).
-- Above it the lookup-table and BMI2 decoders disagree with each other, so an out-of-domain
-- constant here would assert one architecture's answer and fail on the other.
SELECT '----- ROUND TRIP ND=2..8 -----';
select count() from (select number as n from numbers(100000))
where mortonDecode(2, mortonEncode(n % 4294967296, (n*7) % 4294967296))
   != (n % 4294967296, (n*7) % 4294967296);
select count() from (select number as n from numbers(100000))
where mortonDecode(3, mortonEncode(n % 2097152, (n*7) % 2097152, (n*13) % 2097152))
   != (n % 2097152, (n*7) % 2097152, (n*13) % 2097152);
select count() from (select number as n from numbers(100000))
where mortonDecode(4, mortonEncode(n % 65536, (n*7) % 65536, (n*13) % 65536, (n*29) % 65536))
   != (n % 65536, (n*7) % 65536, (n*13) % 65536, (n*29) % 65536);
select count() from (select number as n from numbers(100000))
where mortonDecode(5, mortonEncode(n % 4096, (n*7) % 4096, (n*13) % 4096, (n*29) % 4096, (n*31) % 4096))
   != (n % 4096, (n*7) % 4096, (n*13) % 4096, (n*29) % 4096, (n*31) % 4096);
select count() from (select number as n from numbers(100000))
where mortonDecode(6, mortonEncode(n % 1024, (n*7) % 1024, (n*13) % 1024, (n*29) % 1024, (n*31) % 1024, (n*37) % 1024))
   != (n % 1024, (n*7) % 1024, (n*13) % 1024, (n*29) % 1024, (n*31) % 1024, (n*37) % 1024);
select count() from (select number as n from numbers(100000))
where mortonDecode(7, mortonEncode(n % 512, (n*7) % 512, (n*13) % 512, (n*29) % 512, (n*31) % 512, (n*37) % 512, (n*41) % 512))
   != (n % 512, (n*7) % 512, (n*13) % 512, (n*29) % 512, (n*31) % 512, (n*37) % 512, (n*41) % 512);
select count() from (select number as n from numbers(100000))
where mortonDecode(8, mortonEncode(n % 256, (n*7) % 256, (n*13) % 256, (n*29) % 256, (n*31) % 256, (n*37) % 256, (n*41) % 256, (n*43) % 256))
   != (n % 256, (n*7) % 256, (n*13) % 256, (n*29) % 256, (n*31) % 256, (n*37) % 256, (n*41) % 256, (n*43) % 256);

SELECT '----- MAX IN-DOMAIN CODE -----';
select mortonDecode(2, 0xFFFFFFFFFFFFFFFF);
select mortonDecode(3, 0x7FFFFFFFFFFFFFFF);
select mortonDecode(4, 0xFFFFFFFFFFFFFFFF);
select mortonDecode(5, 0x0FFFFFFFFFFFFFFF);
select mortonDecode(6, 0x0FFFFFFFFFFFFFFF);
select mortonDecode(7, 0x7FFFFFFFFFFFFFFF);
select mortonDecode(8, 0xFFFFFFFFFFFFFFFF);

SELECT '----- CHUNK BOUNDARY -----';
select mortonDecode(3, 0x0000FFFFFFFF0000);
select mortonDecode(5, 0x000FFFFF00000000);
select mortonDecode(6, 0x00000000FFFFFFFF);
select mortonDecode(7, 0x00007FFFFFFF0000);

SELECT '----- END -----';
drop table if exists morton_numbers_02457;

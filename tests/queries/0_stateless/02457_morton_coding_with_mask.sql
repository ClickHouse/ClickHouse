SELECT '----- START -----';

SELECT '----- CONST -----';
select mortonEncode((1,2,3,1), 1,2,3,4);
select mortonDecode((1, 2, 3, 1), 4205569);
select mortonEncode((1,1), 65534, 65533);
select mortonDecode((1,1), 4294967286);
select mortonEncode(tuple(1), 4294967286);
select mortonDecode(tuple(1), 4294967286);
select mortonEncode(tuple(4), 128);
select mortonDecode(tuple(4), 2147483648);
select mortonEncode((4,4,4,4), 128, 128, 128, 128);

SELECT '----- (1,2,1,2) -----';
drop table if exists morton_numbers_mask_02457;
create table morton_numbers_mask_02457(
    n1 UInt8,
    n2 UInt8,
    n3 UInt8,
    n4 UInt8
)
    Engine=MergeTree()
    ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into morton_numbers_mask_02457
select n1.number, n2.number, n3.number, n4.number
from           numbers(256-16, 16) n1
    cross join numbers(256-16, 16) n2
    cross join numbers(256-16, 16) n3
    cross join numbers(256-16, 16) n4
;
drop table if exists morton_numbers_mask_1_02457;
create table morton_numbers_mask_1_02457(
    n1 UInt64,
    n2 UInt64,
    n3 UInt64,
    n4 UInt64
)
    Engine=MergeTree()
    ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into morton_numbers_mask_1_02457
select untuple(mortonDecode((1,2,1,2), mortonEncode((1,2,1,2), n1, n2, n3, n4)))
from morton_numbers_mask_02457;

(
    select * from morton_numbers_mask_02457
    union distinct
    select * from morton_numbers_mask_1_02457
)
except
(
    select * from morton_numbers_mask_02457
    intersect
    select * from morton_numbers_mask_1_02457
);
drop table if exists morton_numbers_mask_02457;
drop table if exists morton_numbers_mask_1_02457;

SELECT '----- (1,4) -----';
drop table if exists morton_numbers_mask_02457;
create table morton_numbers_mask_02457(
    n1 UInt32,
    n2 UInt8
)
    Engine=MergeTree()
    ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into morton_numbers_mask_02457
select n1.number, n2.number
from           numbers(pow(2, 32)-64, 64) n1
    cross join numbers(pow(2, 8)-64, 64) n2
;
drop table if exists morton_numbers_mask_2_02457;
create table morton_numbers_mask_2_02457(
    n1 UInt64,
    n2 UInt64
)
    Engine=MergeTree()
    ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into morton_numbers_mask_2_02457
select untuple(mortonDecode((1,4), mortonEncode((1,4), n1, n2)))
from morton_numbers_mask_02457;

(
    select * from morton_numbers_mask_02457
    union distinct
    select * from morton_numbers_mask_2_02457
)
except
(
    select * from morton_numbers_mask_02457
                      intersect
        select * from morton_numbers_mask_2_02457
);
drop table if exists morton_numbers_mask_02457;
drop table if exists morton_numbers_mask_2_02457;

SELECT '----- (1,1,2) -----';
drop table if exists morton_numbers_mask_02457;
create table morton_numbers_mask_02457(
    n1 UInt16,
    n2 UInt16,
    n3 UInt8,
)
    Engine=MergeTree()
    ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into morton_numbers_mask_02457
select n1.number, n2.number, n3.number
from           numbers(pow(2, 16)-64, 64) n1
                   cross join numbers(pow(2, 16)-64, 64) n2
    cross join numbers(pow(2, 8)-64, 64) n3
;
drop table if exists morton_numbers_mask_3_02457;
create table morton_numbers_mask_3_02457(
    n1 UInt64,
    n2 UInt64,
    n3 UInt64
)
    Engine=MergeTree()
    ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into morton_numbers_mask_3_02457
select untuple(mortonDecode((1,1,2), mortonEncode((1,1,2), n1, n2, n3)))
from morton_numbers_mask_02457;

(
    select * from morton_numbers_mask_02457
    union distinct
    select * from morton_numbers_mask_3_02457
)
except
(
    select * from morton_numbers_mask_02457
    intersect
    select * from morton_numbers_mask_3_02457
);
drop table if exists morton_numbers_mask_02457;
drop table if exists morton_numbers_mask_3_02457;

-- The ratio-mask form is the second consumer of the per-dimension decoders, through
-- FunctionMortonDecode::shrink(). Cover every ratio, staying inside each one's domain.
SELECT '----- SHRINK RATIO 2..8 -----';
select count() from (select number as n from numbers(50000))
where mortonDecode(tuple(2), mortonEncode(tuple(2), n % 4294967296)) != tuple(n % 4294967296);
select count() from (select number as n from numbers(50000))
where mortonDecode(tuple(3), mortonEncode(tuple(3), n % 2097152)) != tuple(n % 2097152);
select count() from (select number as n from numbers(50000))
where mortonDecode(tuple(4), mortonEncode(tuple(4), n % 65536)) != tuple(n % 65536);
select count() from (select number as n from numbers(50000))
where mortonDecode(tuple(5), mortonEncode(tuple(5), n % 4096)) != tuple(n % 4096);
select count() from (select number as n from numbers(50000))
where mortonDecode(tuple(6), mortonEncode(tuple(6), n % 1024)) != tuple(n % 1024);
select count() from (select number as n from numbers(50000))
where mortonDecode(tuple(7), mortonEncode(tuple(7), n % 512)) != tuple(n % 512);
select count() from (select number as n from numbers(50000))
where mortonDecode(tuple(8), mortonEncode(tuple(8), n % 256)) != tuple(n % 256);

SELECT '----- END -----';

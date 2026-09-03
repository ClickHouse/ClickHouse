-- Tags: long

-- test became more than an order of magnitude slower with max_bytes_before_external_sort=1
set max_bytes_before_external_sort = 0;
set max_bytes_ratio_before_external_sort = 0;

drop table if exists stack;

set max_insert_threads = 4;

create table stack(item_id Int64, brand_id Int64, rack_id Int64, dt DateTime, expiration_dt DateTime, quantity UInt64)
Engine = MergeTree
partition by toYYYYMM(dt)
order by (brand_id, toStartOfHour(dt)) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into stack
select number%99991, number%11, number%1111, toDateTime('2020-01-01 00:00:00')+number/100,
   toDateTime('2020-02-01 00:00:00')+number/10, intDiv(number,100)+1
from numbers_mt(1000000);

select '---- arrays ----';

select cityHash64( toString( groupArray (tuple(*) ) )) from (
    select brand_id, rack_id, arrayJoin(arraySlice(arraySort(groupArray(quantity)),1,2)) quantity
    from stack
    group by brand_id, rack_id
    order by brand_id, rack_id, quantity
) t;


select '---- window f ----';

select cityHash64( toString( groupArray (tuple(*) ) )) from (
    select brand_id, rack_id,  quantity from
       ( select brand_id, rack_id, quantity, row_number() over (partition by brand_id, rack_id order by quantity) rn
         from stack ) as t0
    where rn <= 2
    order by brand_id, rack_id, quantity
) t;

drop table if exists stack;

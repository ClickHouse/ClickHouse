-- Tags: no-parallel
drop table if exists foo;

truncate table system.statistics_table sync;
truncate table system.statistics_column_basic sync;

set allow_experimental_object_type = 1;

CREATE TABLE foo
(
    f0  UInt64,
    f1  Float64,
    f2  Decimal32(9),
    f3  Boolean,
    f4  String,
    f5  FixedString(1),
    f6  UUID,
    f7  Date,
    f8  Date32,
    f9  DateTime('Asia/Istanbul'),
    f10 DateTime64(3, 'Asia/Istanbul'),
    f11 Enum8('hello' = 1, 'world' = 2),
    f12 LowCardinality(String),
    f13 Array(UInt8),
    f14 JSON,
    f15 Tuple(UInt8, UInt8),
    f16 Nullable(String),
    f17 IPv4,
    f18 IPv6,
    f19 Point,
    f20 IntervalDay
) Engine = MergeTree() order by f0;

INSERT INTO foo
SELECT 1,
       1.0,
       toDecimal32(1, 9),
       false,
       '1',
       '1',
       '00000000-0000-0000-0000-000000000001',
       '2023-11-01',
       '2023-11-01',
       '2023-11-01 00:00:00',
       '2023-11-01 00:00:00',
       'hello',
       '1',
       array(1, 1),
       '{"a": 1}',
       tuple(1, 1),
       NULL,
       '1.1.1.1',
       '0:0:0:0::1',
       (1, 1),
       1;

INSERT INTO foo
SELECT 2,
       2.0,
       toDecimal32(2, 9),
       true,
       '2',
       '2',
       '00000000-0000-0000-0000-000000000002',
       '2023-11-02',
       '2023-11-02',
       '2023-11-02 00:00:00',
       '2023-11-02 00:00:00',
       'world',
       '2',
       array(2, 2),
       '{"a": 2}',
       tuple(2, 2),
       'ss',
       '1.1.1.2',
       '0:0:0:0::2',
       (2, 2),
       2;

analyze table foo;

select * from system.statistics_table;
select '';

select `table`, `column`, uniqMerge(ndv), any(min_value), any(max_value), any(avg_row_size)
from system.statistics_column_basic
group by `table`, `column`
order by `column`;

-- drop table if exists foo;

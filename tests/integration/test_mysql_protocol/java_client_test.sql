CREATE OR REPLACE TABLE simple_data_types
(
    i8    Int8,
    i16   Int16,
    i32   Int32,
    i64   Int64,
    i128  Int128,
    i256  Int256,
    ui8   UInt8,
    ui16  UInt16,
    ui32  UInt32,
    ui64  UInt64,
    ui128 UInt128,
    ui256 UInt256,
    f32   Float32,
    f64   Float64,
    b     Boolean
) ENGINE MergeTree ORDER BY i8;

INSERT INTO simple_data_types
VALUES (127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727,
        57896044618658097711785492504343953926634992332820282019728792003956564819967,
        255, 65535, 4294967295, 18446744073709551615, 340282366920938463463374607431768211455,
        115792089237316195423570985008687907853269984665640564039457584007913129639935,
        1.234, 3.35245141223232, FALSE),
       (-128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728,
        -57896044618658097711785492504343953926634992332820282019728792003956564819968,
        120, 1234, 51234, 421342, 15324355, 41345135123432,
        -0.7968956, -0.113259, TRUE);

CREATE OR REPLACE TABLE string_types
(
    s   String,
    sn  Nullable(String),
    lc  LowCardinality(String),
    nlc LowCardinality(Nullable(String))
) ENGINE MergeTree ORDER BY s;

INSERT INTO string_types
VALUES ('foo', 'bar', 'qaz', 'qux'),
       ('42', NULL, 'test', NULL);

CREATE OR REPLACE TABLE low_cardinality_and_nullable_types
(
    ilc LowCardinality(Int32),
    dlc LowCardinality(Date),
    ni  Nullable(Int32)
) ENGINE MergeTree ORDER BY ilc;

INSERT INTO low_cardinality_and_nullable_types
VALUES (42, '2011-02-05', NULL),
       (-54, '1970-01-01', 144);

CREATE OR REPLACE TABLE decimal_types
(
    d32         Decimal(9, 2),
    d64         Decimal(18, 3),
    d128_native Decimal(30, 10),
    d128_text   Decimal(38, 31),
    d256        Decimal(76, 20)
) ENGINE MergeTree ORDER BY d32;

INSERT INTO decimal_types
VALUES (1234567.89,
        123456789123456.789,
        12345678912345678912.1234567891,
        1234567.8912345678912345678911234567891,
        12345678912345678912345678911234567891234567891234567891.12345678911234567891),
       (-1.55, 6.03, 5, -1224124.23423, -54342.3);

CREATE OR REPLACE TABLE misc_types
(
    a Array(String),
    u UUID,
    t Tuple(Int32, String),
    m Map(String, Int32)
) ENGINE MergeTree ORDER BY u;

INSERT INTO misc_types
VALUES (['foo', 'bar'], '5da5038d-788f-48c6-b510-babb41c538d3', (42, 'qaz'), {'qux': 144, 'text': 255});

CREATE OR REPLACE TABLE date_types
(
    d      Date,
    d32    Date32,
    dt     DateTime('UTC'),
    dt64_3 DateTime64(3, 'UTC'),
    dt64_6 DateTime64(6, 'UTC'),
    dt64_9 DateTime64(9, 'UTC')
) ENGINE MergeTree ORDER BY d;

INSERT INTO date_types
VALUES ('2149-06-06', '2178-04-16', '2106-02-07 06:28:15',
        '2106-02-07 06:28:15.123',
        '2106-02-07 06:28:15.123456',
        '2106-02-07 06:28:15.123456789'),
        ('1970-01-01', '1900-01-01', '1970-01-01 00:00:00',
        '1900-01-01 00:00:00.001',
        '1900-01-01 00:00:00.000001',
        '1900-01-01 00:00:00.000000001');

CREATE OR REPLACE TABLE unusual_datetime64_scales
(
    dt64_0 DateTime64(0, 'UTC'),
    dt64_1 DateTime64(1, 'UTC'),
    dt64_2 DateTime64(2, 'UTC'),
    dt64_4 DateTime64(4, 'UTC'),
    dt64_5 DateTime64(5, 'UTC'),
    dt64_7 DateTime64(7, 'UTC'),
    dt64_8 DateTime64(8, 'UTC')
) ENGINE MergeTree ORDER BY dt64_0;

INSERT INTO unusual_datetime64_scales
VALUES ('2022-04-13 03:17:45',
        '2022-04-13 03:17:45.1',
        '2022-04-13 03:17:45.12',
        '2022-04-13 03:17:45.1234',
        '2022-04-13 03:17:45.12345',
        '2022-04-13 03:17:45.1234567',
        '2022-04-13 03:17:45.12345678'),
        ('2022-04-13 03:17:45',
        '2022-04-13 03:17:45.1',
        '2022-04-13 03:17:45.01',
        '2022-04-13 03:17:45.0001',
        '2022-04-13 03:17:45.00001',
        '2022-04-13 03:17:45.0000001',
        '2022-04-13 03:17:45.00000001');

CREATE OR REPLACE TABLE datetime_timezones
(
    dt     DateTime('Europe/Amsterdam'),
    dt64_3 DateTime64(3, 'Asia/Shanghai')
) ENGINE MergeTree ORDER BY dt;

INSERT INTO datetime_timezones
VALUES ('2022-09-04 20:31:05', '2022-09-04 20:31:05.022'),
       ('1970-01-01 01:00:00', '1969-12-31 16:00:00');

CREATE OR REPLACE TABLE suspicious_nullable_low_cardinality_types
(
    f  LowCardinality(Nullable(Float32)),
    d  LowCardinality(Nullable(Date)),
    dt LowCardinality(Nullable(DateTime('UTC')))
) ENGINE MergeTree ORDER BY tuple();

INSERT INTO suspicious_nullable_low_cardinality_types
VALUES (1,    '2022-04-15', '2021-06-04 13:55:11'),
       (3.14, '1970-01-01', '1970-01-01 00:00:00'),
       (NULL, NULL, NULL);

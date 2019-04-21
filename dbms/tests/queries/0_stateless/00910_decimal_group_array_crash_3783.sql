select groupArray(s) from (select sum(n) s from (select toDecimal32(1, 2) as n));
select groupArray(s) from (select sum(n) s from (select toDecimal64(1, 5) as n));
select groupArray(s) from (select sum(n) s from (select toDecimal128(1, 10) as n));

select groupArray(s) from (select sum(n) s from (select toDecimal32(number, 2) as n from numbers(1000)));
select groupArray(s) from (select sum(n) s from (select toDecimal64(number, 5) as n from numbers(1000)));
select groupArray(s) from (select sum(n) s from (select toDecimal128(number, 10) as n from numbers(1000)));

DROP TABLE IF EXISTS sensor_value;
CREATE TABLE sensor_value (
    received_at DateTime,
    device_id UUID,
    sensor_id UUID,
    value Nullable(Decimal(18, 4)),
    low_warning Nullable(Decimal(18, 4)),
    low_critical Nullable(Decimal(18, 4)),
    high_warning Nullable(Decimal(18, 4)),
    high_critical Nullable(Decimal(18, 4))
) ENGINE = MergeTree
PARTITION BY toDate(received_at)
ORDER BY (device_id, sensor_id);

INSERT INTO sensor_value (received_at, device_id, sensor_id, value, low_warning, low_critical, high_warning, high_critical) VALUES ('2018-12-18 00:16:07', 'a4d92414-09aa-4dbd-80b2-124ddaacf333', 'ed87e57c-9331-462a-80b4-9f0c005e88c8', '0.4400', '-10000000.0000', '-10000000.0000', '10000000.0000', '10000000.0000');

SELECT `time`, groupArray((sensor_id, volume)) AS groupArr FROM (
    SELECT (intDiv(toUInt32(received_at), 900) * 900) AS `time`, sensor_id, avg(value) AS volume
    FROM sensor_value
    WHERE received_at BETWEEN '2018-12-12 00:00:00' AND '2018-12-30 00:00:00'
    GROUP BY `time`,sensor_id
    ORDER BY `time`
) GROUP BY `time`;

DROP TABLE sensor_value;

select s.a, s.b, max(s.dt1) dt1, s.c, s.d, s.f, s.i, max(s.dt2) dt2 from (
    select toUInt64(4360430)                   a
        , toUInt64(5681495)                    b
        , toDateTime('2018-11-01 10:44:58')    dt1
        , 'txt'                                c
        , toDecimal128('274.350000000000', 12) d
        , toDecimal128(268.970000000000, 12)   f
        , toDecimal128(0.000000000000, 12)     i
        , toDateTime('2018-11-02 00:00:00')    dt2
    union all
    select toUInt64(4341757)                   a
        , toUInt64(5657967)                    b
        , toDateTime('2018-11-01 16:47:46')    dt1
        , 'txt'                                c
        , toDecimal128('321.380000000000', 12) d
        , toDecimal128(315.080000000000, 12)   f
        , toDecimal128(0.000000000000, 12)     i
        , toDateTime('2018-11-02 00:00:00')    dt2
    union all
    select toUInt64(4360430)                   a
        , toUInt64(5681495)                    b
        , toDateTime('2018-11-02 09:00:07')    dt1
        , 'txt'                                c
        , toDecimal128('274.350000000000', 12) d
        , toDecimal128(268.970000000000, 12)   f
        , toDecimal128(0.000000000000, 12)     i
        , toDateTime('2018-11-02 00:00:00')    dt2
) s group by s.a, s.b, s.c, s.d, s.f, s.i;

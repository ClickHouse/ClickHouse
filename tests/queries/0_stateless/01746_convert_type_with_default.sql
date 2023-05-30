select toUInt8OrDefault('1', cast(2 as UInt8));
select toUInt8OrDefault('1xx', cast(2 as UInt8));
select toInt8OrDefault('-1', cast(-2 as Int8));
select toInt8OrDefault('-1xx', cast(-2 as Int8));

select toUInt16OrDefault('1', cast(2 as UInt16));
select toUInt16OrDefault('1xx', cast(2 as UInt16));
select toInt16OrDefault('-1', cast(-2 as Int16));
select toInt16OrDefault('-1xx', cast(-2 as Int16));

select toUInt32OrDefault('1', cast(2 as UInt32));
select toUInt32OrDefault('1xx', cast(2 as UInt32));
select toInt32OrDefault('-1', cast(-2 as Int32));
select toInt32OrDefault('-1xx', cast(-2 as Int32));

select toUInt64OrDefault('1', cast(2 as UInt64));
select toUInt64OrDefault('1xx', cast(2 as UInt64));
select toInt64OrDefault('-1', cast(-2 as Int64));
select toInt64OrDefault('-1xx', cast(-2 as Int64));

select toInt128OrDefault('-1', cast(-2 as Int128));
select toInt128OrDefault('-1xx', cast(-2 as Int128));

select toUInt256OrDefault('1', cast(2 as UInt256));
select toUInt256OrDefault('1xx', cast(2 as UInt256));
select toInt256OrDefault('-1', cast(-2 as Int256));
select toInt256OrDefault('-1xx', cast(-2 as Int256));

select toDateOrDefault('2020-0x-02');
select toDateOrDefault('2023-05-30');
select toDateOrDefault('2023-05-30', '2000-01-01'::Date);
select toDateOrDefault('2020-0x-02', '2023-05-30'::Date);
select toDateOrDefault(-1);
select toDateOrDefault(19507);
select toDateOrDefault(19507, '2000-01-01'::Date);
select toDateOrDefault(-1, '2000-01-01'::Date);

select toDateTimeOrDefault('2023-05-30 14:38:20');
select toDateTimeOrDefault('2023-05-30 14:38:20', 'UTC');
select toDateTimeOrDefault('s2023', 'UTC', '2023-05-30 14:38:20'::DateTime('UTC'));
select toDateTimeOrDefault(1685457500);

SELECT toUUIDOrDefault('61f0c404-5cb3-11e7-907b-a6006ad3dba0', cast('59f0c404-5cb3-11e7-907b-a6006ad3dba0' as UUID));
SELECT toUUIDOrDefault('-----61f0c404-5cb3-11e7-907b-a6006ad3dba0', cast('59f0c404-5cb3-11e7-907b-a6006ad3dba0' as UUID));
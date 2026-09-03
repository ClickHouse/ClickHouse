SET send_logs_level = 'fatal';

-- { echoOn }
DROP TABLE IF EXISTS sum_map;
CREATE TABLE sum_map(date Date, timeslot DateTime, statusMap Nested(status UInt16, requests UInt64)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO sum_map VALUES ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]), ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]), ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]), ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);

SELECT * FROM sum_map ORDER BY timeslot, statusMap.status, statusMap.requests;
SELECT sumMap(statusMap.status, statusMap.requests) FROM sum_map;
SELECT sumMap((statusMap.status, statusMap.requests)) FROM sum_map;
SELECT sumMapMerge(s) FROM (SELECT sumMapState(statusMap.status, statusMap.requests) AS s FROM sum_map);
SELECT timeslot, sumMap(statusMap.status, statusMap.requests) FROM sum_map GROUP BY timeslot ORDER BY timeslot;
SELECT timeslot, sumMap(statusMap.status, statusMap.requests).1, sumMap(statusMap.status, statusMap.requests).2 FROM sum_map GROUP BY timeslot ORDER BY timeslot;

SELECT sumMapFiltered([1])(statusMap.status, statusMap.requests) FROM sum_map;
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) FROM sum_map;

DROP TABLE sum_map;

DROP TABLE IF EXISTS sum_map_overflow;
CREATE TABLE sum_map_overflow(events Array(UInt8), counts Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO sum_map_overflow VALUES ([1], [255]), ([1], [2]);

SELECT sumMap(events, counts) FROM sum_map_overflow;
SELECT sumMapWithOverflow(events, counts) FROM sum_map_overflow;

DROP TABLE sum_map_overflow;

select sumMap(val, cnt) from ( SELECT [ CAST(1, 'UInt64') ] as val, [1] as cnt );
select sumMap(val, cnt) from ( SELECT [ CAST(1, 'Float64') ] as val, [1] as cnt );
select sumMap(val, cnt) from ( SELECT [ CAST('a', 'Enum16(\'a\'=1)') ] as val, [1] as cnt );

select sumMap(val, cnt) from ( SELECT [ CAST(1, 'DateTime(\'Asia/Istanbul\')') ] as val, [1] as cnt );
select sumMap(val, cnt) from ( SELECT [ CAST(1, 'Date') ] as val, [1] as cnt );
select sumMap(val, cnt) from ( SELECT [ CAST('01234567-89ab-cdef-0123-456789abcdef', 'UUID') ] as val, [1] as cnt );
select sumMap(val, cnt) from ( SELECT [ CAST(1.01, 'Decimal(10,2)') ] as val, [1] as cnt );

select sumMap(val, cnt) from ( SELECT [ CAST('a', 'FixedString(1)'), CAST('b', 'FixedString(1)' ) ] as val, [1, 2] as cnt );
select sumMap(val, cnt) from ( SELECT [ CAST('abc', 'String'), CAST('ab', 'String'), CAST('a', 'String') ] as val, [1, 2, 3] as cnt );

DROP TABLE IF EXISTS sum_map_decimal;

CREATE TABLE sum_map_decimal(
    statusMap Nested(
        goal_id UInt16,
        revenue Decimal32(5)
    )
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO sum_map_decimal VALUES ([1, 2, 3], [1.0, 2.0, 3.0]), ([3, 4, 5], [3.0, 4.0, 5.0]), ([4, 5, 6], [4.0, 5.0, 6.0]), ([6, 7, 8], [6.0, 7.0, 8.0]);

SELECT sumMap(statusMap.goal_id, statusMap.revenue) FROM sum_map_decimal;
SELECT sumMapWithOverflow(statusMap.goal_id, statusMap.revenue) FROM sum_map_decimal;

DROP TABLE sum_map_decimal;

CREATE TABLE sum_map_decimal_nullable (`statusMap` Nested(goal_id UInt16, revenue Nullable(Decimal(9, 5)))) engine=MergeTree ORDER BY tuple();
INSERT INTO sum_map_decimal_nullable VALUES ([1, 2, 3], [1.0, 2.0, 3.0]), ([3, 4, 5], [3.0, 4.0, 5.0]), ([4, 5, 6], [4.0, 5.0, 6.0]), ([6, 7, 8], [6.0, 7.0, 8.0]);
SELECT sumMap(statusMap.goal_id, statusMap.revenue) FROM sum_map_decimal_nullable;
DROP TABLE sum_map_decimal_nullable;

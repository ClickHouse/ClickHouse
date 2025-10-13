SET send_logs_level = 'fatal';

-- { echoOn }
DROP TABLE IF EXISTS uniq_map;

-- Test basic functionality with integers
CREATE TABLE uniq_map(date Date, timeslot DateTime, statusMap Nested(status UInt16, requests UInt64)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO uniq_map VALUES 
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]), 
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]), 
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]), 
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);

-- Test basic uniqMap - should count unique values per key
SELECT uniqMap(statusMap.status, statusMap.requests) FROM uniq_map;

-- Test with tuple argument
SELECT uniqMap((statusMap.status, statusMap.requests)) FROM uniq_map;

-- Test grouped by timeslot
SELECT timeslot, uniqMap(statusMap.status, statusMap.requests) FROM uniq_map GROUP BY timeslot ORDER BY timeslot;

-- Test accessing tuple elements
SELECT timeslot, uniqMap(statusMap.status, statusMap.requests).1, uniqMap(statusMap.status, statusMap.requests).2 FROM uniq_map GROUP BY timeslot ORDER BY timeslot;

DROP TABLE uniq_map;

-- Test with duplicate values to verify uniqueness counting
DROP TABLE IF EXISTS uniq_map_duplicates;
CREATE TABLE uniq_map_duplicates(keys Array(UInt64), values Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO uniq_map_duplicates VALUES
    ([1, 2, 3], [100, 200, 300]),
    ([1, 2, 3], [100, 200, 300]),
    ([1, 2, 3], [100, 250, 300]),
    ([4, 5], [400, 500]);

-- Key 1 should have 1 unique value (100)
-- Key 2 should have 2 unique values (200, 250)
-- Key 3 should have 1 unique value (300)
-- Key 4 should have 1 unique value (400)
-- Key 5 should have 1 unique value (500)
SELECT uniqMap(keys, values) FROM uniq_map_duplicates;

DROP TABLE uniq_map_duplicates;

-- Test with multiple value arrays
DROP TABLE IF EXISTS uniq_map_multi;
CREATE TABLE uniq_map_multi(
    date Date,
    browser_metrics Nested(
        browser String,
        impressions UInt32,
        clicks UInt32
    )
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO uniq_map_multi VALUES
    ('2000-01-01', ['Firefox', 'Chrome'], [100, 200], [10, 25]),
    ('2000-01-01', ['Chrome', 'Safari'], [200, 50], [25, 5]),
    ('2000-01-01', ['Firefox', 'Edge'], [100, 40], [15, 4]);

-- Firefox: 1 unique impression (100), 2 unique clicks (10, 15)
-- Chrome: 1 unique impression (200), 1 unique click (25)
-- Safari: 1 unique impression (50), 1 unique click (5)
-- Edge: 1 unique impression (40), 1 unique click (4)
SELECT uniqMap(browser_metrics.browser, browser_metrics.impressions, browser_metrics.clicks) FROM uniq_map_multi;

DROP TABLE uniq_map_multi;

-- Test with different data types
select uniqMap(val, cnt) from ( SELECT [ CAST(1, 'UInt64') ] as val, [1] as cnt );
select uniqMap(val, cnt) from ( SELECT [ CAST(1, 'Float64') ] as val, [1] as cnt );
select uniqMap(val, cnt) from ( SELECT [ CAST('a', 'Enum16(\'a\'=1)') ] as val, [1] as cnt );

-- Test with strings
select uniqMap(val, cnt) from ( SELECT [ CAST('a', 'FixedString(1)'), CAST('b', 'FixedString(1)' ) ] as val, [1, 2] as cnt );
select uniqMap(val, cnt) from ( SELECT [ CAST('abc', 'String'), CAST('ab', 'String'), CAST('a', 'String') ] as val, [1, 2, 3] as cnt );

-- Test with nulls
DROP TABLE IF EXISTS uniq_map_nulls;
CREATE TABLE uniq_map_nulls(keys Array(UInt64), values Array(Nullable(UInt64))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO uniq_map_nulls VALUES 
    ([1, 2, 3], [100, 200, NULL]),
    ([1, 2, 3], [100, NULL, 300]),
    ([1, 2], [150, 200]);

-- Key 1: 2 unique values (100, 150)
-- Key 2: 1 unique value (200) - nulls are ignored
-- Key 3: 1 unique value (300) - nulls are ignored
SELECT uniqMap(keys, values) FROM uniq_map_nulls;

DROP TABLE uniq_map_nulls;

-- Test state functions
DROP TABLE IF EXISTS uniq_map_state;
CREATE TABLE uniq_map_state(keys Array(UInt64), values Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO uniq_map_state VALUES 
    ([1, 2], [100, 200]),
    ([2, 3], [200, 300]);

SELECT uniqMapMerge(s) FROM (SELECT uniqMapState(keys, values) AS s FROM uniq_map_state);

DROP TABLE uniq_map_state;


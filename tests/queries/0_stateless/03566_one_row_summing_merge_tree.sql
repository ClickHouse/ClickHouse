DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    key UInt32,
    A UInt32,
    B String
)
ENGINE = SummingMergeTree()
ORDER BY key;

INSERT INTO test_table Values(1,0,'');

SELECT count() FROM test_table;

DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    key UInt32,
    A UInt32,
    B String
)
ENGINE = CoalescingMergeTree()
ORDER BY key;

INSERT INTO test_table Values(1,0,'');

SELECT count() FROM test_table;

DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    key UInt32,
    A Nullable(UInt32),
    B Nullable(String)
)
ENGINE = CoalescingMergeTree()
ORDER BY key;

INSERT INTO test_table Values(1, 1,  'x');
SELECT * FROM test_table FINAL ORDER BY ALL;

INSERT INTO test_table Values(1, 2,  'y');
SELECT * FROM test_table FINAL ORDER BY ALL;

INSERT INTO test_table Values(1, 3,  'z');
SELECT * FROM test_table FINAL ORDER BY ALL;

DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    key UInt32,
    value Nullable(UInt32)
)
ENGINE = CoalescingMergeTree()
ORDER BY key;

INSERT INTO test_table VALUES(1,6);   
INSERT INTO test_table VALUES(1,NULL); 
select * from test_table final;

truncate table test_table;

INSERT INTO test_table VALUES(1,6), (1,NULL);   
select * from test_table final;

select ' -- AggregatingMergeTree --\n' format TSVRaw;
DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    key UInt32,
    A SimpleAggregateFunction(anyLast,Nullable(Int64)),  
    B SimpleAggregateFunction(anyLast,Nullable(DateTime)),
    C SimpleAggregateFunction(anyLast,Nullable(String))
)
ENGINE = AggregatingMergeTree()
ORDER BY key;

INSERT INTO test_table(key, A) values(1, 1);
--SELECT * FROM test_table final format PrettyCompact; 
INSERT INTO test_table(key, B) values(1, '2020-01-01');
--SELECT * FROM test_table final format PrettyCompact; 
INSERT INTO test_table(key, C) values(1, 'a');
--SELECT * FROM test_table final format PrettyCompact; 
INSERT INTO test_table(key, B) values(1, '2022-01-01');
--SELECT * FROM test_table final format PrettyCompact; 
INSERT INTO test_table(key, A) values(1, 5);
--SELECT * FROM test_table final format PrettyCompact; 
INSERT INTO test_table(key, B) values(1, Null);
--SELECT * FROM test_table final format PrettyCompact; 
INSERT INTO test_table(key, A, C) values(1, Null, Null);
SELECT * FROM test_table final; 

select '\n\n -- CoalescingMergeTree --\n' format TSVRaw;

drop table test_table;
CREATE TABLE test_table
(
    key UInt32,
    A Nullable(Int64),  
    B Nullable(DateTime),
    C Nullable(String),
)
ENGINE = CoalescingMergeTree()
ORDER BY key;

INSERT INTO test_table(key, A) values(1, 1);
--SELECT * FROM test_table final format PrettyCompact; 
INSERT INTO test_table(key, B) values(1, '2020-01-01');
--SELECT * FROM test_table final format PrettyCompact; 
INSERT INTO test_table(key, C) values(1, 'a');
--SELECT * FROM test_table final format PrettyCompact; 
INSERT INTO test_table(key, B) values(1, '2022-01-01');
--SELECT * FROM test_table final format PrettyCompact; 
INSERT INTO test_table(key, A) values(1, 5);
--SELECT * FROM test_table final format PrettyCompact; 
INSERT INTO test_table(key, B) values(1, Null);
--SELECT * FROM test_table final format PrettyCompact; 
INSERT INTO test_table(key, A, C) values(1, Null, Null);
SELECT * FROM test_table final; 


CREATE TABLE electric_vehicle_state
(
    vin String, -- vehicle identification number
    last_update DateTime64 Materialized now64(), -- optional (used with argMax)
    battery_level Nullable(UInt8), -- in %
    lat Nullable(Float64), -- latitude (°)
    lon Nullable(Float64), -- longitude (°)
    firmware_version Nullable(String),
    cabin_temperature Nullable(Float32), -- in °C
    speed_kmh Nullable(Float32) -- from sensor
)
ENGINE = CoalescingMergeTree
ORDER BY vin;

-- ① Initial battery and firmware readings
INSERT INTO electric_vehicle_state VALUES
('5YJ3E1EA7KF000001', 82, NULL, NULL, '2024.14.5', NULL, NULL);

-- ② GPS reports in later
INSERT INTO electric_vehicle_state VALUES
('5YJ3E1EA7KF000001', NULL, 37.7749, -122.4194, NULL, NULL, NULL);

-- ③ Sensor update: temperature + speed
INSERT INTO electric_vehicle_state VALUES
('5YJ3E1EA7KF000001', NULL, NULL, NULL, NULL, 22.5, 67.3);

-- ④ Battery drops to 78%
INSERT INTO electric_vehicle_state VALUES
('5YJ3E1EA7KF000001', 78, NULL, NULL, NULL, NULL, NULL);

-- ⑤ Another car, initial firmware and temp readings
INSERT INTO electric_vehicle_state VALUES
('5YJ3E1EA7KF000099', NULL, NULL, NULL, '2024.14.5', 19.2, NULL);

INSERT INTO electric_vehicle_state VALUES
('5YJ3E1EA7KF000099', NULL, NULL, NULL, '2025.38.46', 19.3, NULL);

SELECT
    vin,
    battery_level AS batt,
    lat AS lat,
    lon AS lon,
    firmware_version AS fw,
    cabin_temperature AS temp,
    speed_kmh AS speed
FROM electric_vehicle_state FINAL
ORDER BY vin;

DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    key UInt32,
    value Nullable(UInt32),
    value_arr Variant(Array(UInt32), Nothing)
)
ENGINE = CoalescingMergeTree()
ORDER BY key;


INSERT INTO test_table VALUES(1,6, NULL);   
INSERT INTO test_table VALUES(1,NULL, [1]); 

select * from test_table final;

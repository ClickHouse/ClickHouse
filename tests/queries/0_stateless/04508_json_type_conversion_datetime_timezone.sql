SET session_timezone = 'UTC';
SET json_use_optimized_type_conversion = 1;

-- Test 1: DateTime('Asia/Tokyo') removed typed path — serialized in Tokyo timezone,
-- re-parsed in session timezone (UTC). Text stays the same, epoch shifts.
SELECT 'Test 1: DateTime with explicit timezone removal';
SELECT '{"dt":"2024-01-15 21:30:00"}'::JSON(dt DateTime('Asia/Tokyo')) as json;
SELECT ('{"dt":"2024-01-15 21:30:00"}'::JSON(dt DateTime('Asia/Tokyo'))::JSON).dt as dt,
       dynamicType(('{"dt":"2024-01-15 21:30:00"}'::JSON(dt DateTime('Asia/Tokyo'))::JSON).dt) as t;

-- Test 2: DateTime64(3, 'Asia/Tokyo') removed typed path — precision changes to 9.
SELECT 'Test 2: DateTime64 with explicit timezone removal';
SELECT '{"dt":"2024-01-15 21:30:00.123"}'::JSON(dt DateTime64(3, 'Asia/Tokyo')) as json;
SELECT ('{"dt":"2024-01-15 21:30:00.123"}'::JSON(dt DateTime64(3, 'Asia/Tokyo'))::JSON).dt as dt,
       dynamicType(('{"dt":"2024-01-15 21:30:00.123"}'::JSON(dt DateTime64(3, 'Asia/Tokyo'))::JSON).dt) as t;

-- Test 3: DateTime('Asia/Tokyo') changed typed path to String.
SELECT 'Test 3: DateTime with explicit timezone, changed typed path';
SELECT '{"dt":"2024-01-15 21:30:00"}'::JSON(dt DateTime('Asia/Tokyo')) as json;
SELECT ('{"dt":"2024-01-15 21:30:00"}'::JSON(dt DateTime('Asia/Tokyo'))::JSON(dt String)).dt as dt;

-- Test 4: date_time_output_format = 'UnixTimestamp' — DateTime serialized as quoted epoch,
-- DynamicNode infers String.
SELECT 'Test 4: date_time_output_format = UnixTimestamp, removal';
SET date_time_output_format = 'unix_timestamp';
SELECT ('{"dt":"2024-01-15 12:30:00"}'::JSON(dt DateTime)::JSON).dt as dt,
       dynamicType(('{"dt":"2024-01-15 12:30:00"}'::JSON(dt DateTime)::JSON).dt) as t;
SET date_time_output_format = 'simple';

-- Test 5: DateTime without explicit timezone — fast path preserves DateTime type.
SELECT 'Test 5: DateTime without explicit timezone (fast path)';
SELECT '{"dt":"2024-01-15 12:30:00"}'::JSON(dt DateTime)::JSON as json;
SELECT ('{"dt":"2024-01-15 12:30:00"}'::JSON(dt DateTime)::JSON).dt as dt,
       dynamicType(('{"dt":"2024-01-15 12:30:00"}'::JSON(dt DateTime)::JSON).dt) as t;

-- Test 6: date_time_output_format = 'ISO' — DateTime serialized as ISO string,
-- DynamicNode infers DateTime.
SELECT 'Test 6: date_time_output_format = ISO, removal';
SET date_time_output_format = 'iso';
SELECT ('{"dt":"2024-01-15 12:30:00"}'::JSON(dt DateTime)::JSON).dt as dt,
       dynamicType(('{"dt":"2024-01-15 12:30:00"}'::JSON(dt DateTime)::JSON).dt) as t;
SET date_time_output_format = 'simple';

-- Test 7: Array(DateTime('Asia/Tokyo')) removed typed path.
SELECT 'Test 7: Array(DateTime) with explicit timezone removal';
SELECT '{"dt":["2024-01-15 21:30:00","2024-01-15 22:00:00"]}'::JSON(dt Array(DateTime('Asia/Tokyo')))::JSON as json;

-- Test 8: Array(DateTime('Asia/Tokyo')) changed typed path to Array(String).
SELECT 'Test 8: Array(DateTime) with explicit timezone, changed to Array(String)';
SELECT '{"dt":["2024-01-15 21:30:00"]}'::JSON(dt Array(DateTime('Asia/Tokyo')))::JSON(dt Array(String)) as json;

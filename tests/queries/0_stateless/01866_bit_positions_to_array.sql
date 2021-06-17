SELECT 'Int8';
SELECT toInt8(0), bitPositionsToArray(toInt8(0));
SELECT toInt8(1), bitPositionsToArray(toInt8(1));
SELECT toInt8(-1), bitPositionsToArray(toInt8(-1));
SELECT toInt8(127), bitPositionsToArray(toInt8(127));
SELECT toInt8(128), bitPositionsToArray(toInt8(128));

SELECT 'Int16';
SELECT toInt16(0), bitPositionsToArray(toInt16(0));
SELECT toInt16(1), bitPositionsToArray(toInt16(1));
SELECT toInt16(-1), bitPositionsToArray(toInt16(-1));
select toInt16(32765), bitPositionsToArray(toInt16(32765));
select toInt16(32768), bitPositionsToArray(toInt16(32768));

SELECT 'Int32';
SELECT toInt32(0), bitPositionsToArray(toInt32(0));
SELECT toInt32(1), bitPositionsToArray(toInt32(1));

SELECT 'Int64';
SELECT toInt64(0), bitPositionsToArray(toInt64(0));
SELECT toInt64(1), bitPositionsToArray(toInt64(1));

SELECT 'UInt8';
SELECT toUInt8(0), bitPositionsToArray(toUInt8(0));
SELECT toUInt8(1), bitPositionsToArray(toUInt8(1));
SELECT toUInt8(128), bitPositionsToArray(toUInt8(128));

SELECT 'UInt16';
SELECT toUInt16(0), bitPositionsToArray(toUInt16(0));
SELECT toUInt16(1), bitPositionsToArray(toUInt16(1));

SELECT 'UInt32';
SELECT toUInt32(0), bitPositionsToArray(toUInt32(0));
SELECT toUInt32(1), bitPositionsToArray(toUInt32(1));

SELECT 'UInt64';
SELECT toUInt64(0), bitPositionsToArray(toUInt64(0));
SELECT toUInt64(1), bitPositionsToArray(toUInt64(1));

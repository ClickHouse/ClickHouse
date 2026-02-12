-- Test MacAddress data type
SET allow_experimental_macaddress_type = 1;

-- Test 1: Create table with MacAddress column
DROP TABLE IF EXISTS test_macaddress;
CREATE TABLE test_macaddress
(
    id UInt32,
    mac MacAddress
) ENGINE = Memory;

-- Test 2: Insert MAC addresses in various formats
INSERT INTO test_macaddress VALUES (1, toMacAddress('00:1A:2B:3C:4D:5E'));
INSERT INTO test_macaddress VALUES (2, toMacAddress('00-1A-2B-3C-4D-5E'));
INSERT INTO test_macaddress VALUES (3, toMacAddress('001A.2B3C.4D5E'));
INSERT INTO test_macaddress VALUES (4, toMacAddress('001A2B3C4D5E'));
INSERT INTO test_macaddress VALUES (5, toMacAddress('FF:FF:FF:FF:FF:FF'));
INSERT INTO test_macaddress VALUES (6, toMacAddress('00:00:00:00:00:00'));

-- Test 3: Select and verify output format
SELECT 'Select and verify output format';
SELECT id, mac FROM test_macaddress ORDER BY id;

-- Test 4: Test comparison operators
SELECT 'Test comparison operators';
SELECT id FROM test_macaddress WHERE mac = toMacAddress('00:1a:2b:3c:4d:5e') ORDER BY id;
SELECT id FROM test_macaddress WHERE mac < toMacAddress('FF:FF:FF:FF:FF:FF') ORDER BY id;
SELECT id FROM test_macaddress WHERE mac > toMacAddress('00:00:00:00:00:00') ORDER BY id;

-- Test 5: Test GROUP BY
SELECT 'Test GROUP BY';
SELECT mac, count() FROM test_macaddress GROUP BY mac ORDER BY mac;

-- Test 6: Test ORDER BY
SELECT 'Test ORDER BY';
SELECT id, mac FROM test_macaddress ORDER BY mac, id;

-- Test 7: Test DISTINCT
SELECT 'Test DISTINCT';
SELECT DISTINCT mac FROM test_macaddress ORDER BY mac;

-- Test 8: Test with NULL values
DROP TABLE IF EXISTS test_macaddress_nullable;
CREATE TABLE test_macaddress_nullable
(
    id UInt32,
    mac Nullable(MacAddress)
) ENGINE = Memory;

INSERT INTO test_macaddress_nullable VALUES (1, toMacAddress('00:1A:2B:3C:4D:5E'));
INSERT INTO test_macaddress_nullable VALUES (2, NULL);
INSERT INTO test_macaddress_nullable VALUES (3, toMacAddress('FF:FF:FF:FF:FF:FF'));

SELECT 'Test with NULL values';
SELECT id, mac FROM test_macaddress_nullable ORDER BY id;
SELECT id FROM test_macaddress_nullable WHERE mac IS NULL;
SELECT id FROM test_macaddress_nullable WHERE mac IS NOT NULL ORDER BY id;

-- Test 9: Test JOIN
DROP TABLE IF EXISTS test_macaddress_join;
CREATE TABLE test_macaddress_join
(
    mac MacAddress,
    device_name String
) ENGINE = Memory;

INSERT INTO test_macaddress_join VALUES (toMacAddress('00:1A:2B:3C:4D:5E'), 'Device A');
INSERT INTO test_macaddress_join VALUES (toMacAddress('FF:FF:FF:FF:FF:FF'), 'Broadcast');

SELECT 'Test JOIN';
SELECT t1.id, t2.device_name
FROM test_macaddress t1
JOIN test_macaddress_join t2 ON t1.mac = t2.mac
ORDER BY t1.id;

-- Test 10: Test with Array
DROP TABLE IF EXISTS test_macaddress_array;
CREATE TABLE test_macaddress_array
(
    id UInt32,
    macs Array(MacAddress)
) ENGINE = Memory;

INSERT INTO test_macaddress_array VALUES (1, [toMacAddress('00:1A:2B:3C:4D:5E'), toMacAddress('FF:FF:FF:FF:FF:FF')]);
INSERT INTO test_macaddress_array VALUES (2, [toMacAddress('00:00:00:00:00:00')]);

SELECT 'Test with Array';
SELECT id, macs FROM test_macaddress_array ORDER BY id;

-- Test 11: Test conversion from integer to MacAddress
SELECT 'Test conversion from integer to MacAddress';
SELECT toMacAddress(112394521950); -- 00:1A:2B:3C:4D:5E
SELECT toMacAddress(0);
SELECT toMacAddress(281474976710655); -- FF:FF:FF:FF:FF:FF

-- Test 12: Test conversion TO UInt64
SELECT 'Test conversion TO UInt64';
SELECT toUInt64(toMacAddress('00:1A:2B:3C:4D:5E'));
SELECT toUInt64(toMacAddress('FF:FF:FF:FF:FF:FF'));
SELECT toUInt64(toMacAddress('00:00:00:00:00:00'));

-- Test 13: Test conversion TO String
SELECT 'Test conversion TO String';
SELECT toString(toMacAddress('00:1A:2B:3C:4D:5E'));
SELECT toString(toMacAddress('FF:FF:FF:FF:FF:FF'));

-- Test 14: Test CAST operations
SELECT 'Test CAST operations';
SELECT CAST('00:1A:2B:3C:4D:5E' AS MacAddress);
SELECT CAST(112394521950 AS MacAddress);
SELECT CAST(toMacAddress('00:1A:2B:3C:4D:5E') AS UInt64);
SELECT CAST(toMacAddress('00:1A:2B:3C:4D:5E') AS String);

-- Test 15: Test toMacAddressOrZero
SELECT 'Test toMacAddressOrZero';
SELECT toMacAddressOrZero('00:1A:2B:3C:4D:5E');
SELECT toMacAddressOrZero('invalid');
SELECT toMacAddressOrZero('');
SELECT toMacAddressOrZero('ZZ:ZZ:ZZ:ZZ:ZZ:ZZ');

-- Test 16: Test toMacAddressOrNull
SELECT 'Test toMacAddressOrNull';
SELECT toMacAddressOrNull('00:1A:2B:3C:4D:5E');
SELECT toMacAddressOrNull('invalid');
SELECT toMacAddressOrNull('');
SELECT toMacAddressOrNull('ZZ:ZZ:ZZ:ZZ:ZZ:ZZ');

-- Test 17: Test error cases for toMacAddress with invalid inputs
SELECT 'Test error cases for toMacAddress';
SELECT toMacAddress('invalid'); -- { serverError CANNOT_PARSE_MAC_ADDRESS }
SELECT toMacAddress(''); -- { serverError CANNOT_PARSE_MAC_ADDRESS }
SELECT toMacAddress('ZZ:ZZ:ZZ:ZZ:ZZ:ZZ'); -- { serverError CANNOT_PARSE_MAC_ADDRESS }
SELECT toMacAddress('00:1A:2B:3C:4D'); -- { serverError CANNOT_PARSE_MAC_ADDRESS }
SELECT toMacAddress('00:1A:2B:3C:4D:5E:6F'); -- { serverError CANNOT_PARSE_MAC_ADDRESS }

-- Test 18: Test LowCardinality(MacAddress)
DROP TABLE IF EXISTS test_macaddress_lowcardinality;
CREATE TABLE test_macaddress_lowcardinality
(
    id UInt32,
    mac LowCardinality(MacAddress)
) ENGINE = Memory;

INSERT INTO test_macaddress_lowcardinality VALUES (1, toMacAddress('00:1A:2B:3C:4D:5E'));
INSERT INTO test_macaddress_lowcardinality VALUES (2, toMacAddress('00:1A:2B:3C:4D:5E'));
INSERT INTO test_macaddress_lowcardinality VALUES (3, toMacAddress('FF:FF:FF:FF:FF:FF'));
INSERT INTO test_macaddress_lowcardinality VALUES (4, toMacAddress('00:00:00:00:00:00'));

SELECT 'Test LowCardinality(MacAddress)';
SELECT id, mac FROM test_macaddress_lowcardinality ORDER BY id;
SELECT mac, count() FROM test_macaddress_lowcardinality GROUP BY mac ORDER BY mac;

-- Cleanup
DROP TABLE IF EXISTS test_macaddress;
DROP TABLE IF EXISTS test_macaddress_nullable;
DROP TABLE IF EXISTS test_macaddress_join;
DROP TABLE IF EXISTS test_macaddress_array;
DROP TABLE IF EXISTS test_macaddress_lowcardinality;


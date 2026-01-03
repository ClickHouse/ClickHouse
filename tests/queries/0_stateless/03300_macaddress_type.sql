-- Test MacAddress data type

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

-- Cleanup
DROP TABLE IF EXISTS test_macaddress;
DROP TABLE IF EXISTS test_macaddress_nullable;
DROP TABLE IF EXISTS test_macaddress_join;
DROP TABLE IF EXISTS test_macaddress_array;


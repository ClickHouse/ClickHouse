DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (id UInt64, value_ipv4 IPv4, value_ipv6 IPv6) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, '127.0.0.1', '127.0.0.1');

SELECT id FROM test_table WHERE value_ipv4 IN (SELECT value_ipv4 FROM test_table);
SELECT id FROM test_table WHERE value_ipv6 IN (SELECT value_ipv6 FROM test_table);

-- Out-of-range UInt64 values must not be included in an IPv4 IN set.
SELECT toIPv4('1.2.3.4') IN (4294967296);
SELECT toIPv4('1.2.3.4') IN (16909060, 4294967296);
SELECT toIPv4('1.2.3.4') NOT IN (4294967296);
SELECT toIPv4('1.2.3.4') IN (SELECT 4294967296);
SELECT toIPv4('255.255.255.255') IN (4294967295, 4294967296);
SELECT value_ipv4 IN (4294967296) FROM test_table;
SELECT value_ipv4 NOT IN (4294967296) FROM test_table;

DROP TABLE test_table;

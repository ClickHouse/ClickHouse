SELECT '# Invocation with constants';

SELECT isIPAddressInRange('127.0.0.1', '127.0.0.0/8');
SELECT isIPAddressInRange('128.0.0.1', '127.0.0.0/8');

SELECT isIPAddressInRange('ffff::1', 'ffff::/16');
SELECT isIPAddressInRange('fffe::1', 'ffff::/16');

SELECT '# Invocation with non-constant addresses';

WITH arrayJoin(['192.168.99.255', '192.168.100.1', '192.168.103.255', '192.168.104.0']) as addr, '192.168.100.0/22' as prefix SELECT addr, prefix, isIPAddressInRange(addr, prefix);
WITH arrayJoin(['::192.168.99.255', '::192.168.100.1', '::192.168.103.255', '::192.168.104.0']) as addr, '::192.168.100.0/118' as prefix SELECT addr, prefix, isIPAddressInRange(addr, prefix);

SELECT '# Invocation with non-constant prefixes';

WITH '192.168.100.1' as addr, arrayJoin(['192.168.100.0/22', '192.168.100.0/24', '192.168.100.0/32']) as prefix SELECT addr, prefix, isIPAddressInRange(addr, prefix);
WITH '::192.168.100.1' as addr, arrayJoin(['::192.168.100.0/118', '::192.168.100.0/120', '::192.168.100.0/128']) as prefix SELECT addr, prefix, isIPAddressInRange(addr, prefix);

SELECT '# Invocation with non-constants';

WITH arrayJoin(['192.168.100.1', '192.168.103.255']) as addr, arrayJoin(['192.168.100.0/22', '192.168.100.0/24']) as prefix SELECT addr, prefix, isIPAddressInRange(addr, prefix);
WITH arrayJoin(['::192.168.100.1', '::192.168.103.255']) as addr, arrayJoin(['::192.168.100.0/118', '::192.168.100.0/120']) as prefix SELECT addr, prefix, isIPAddressInRange(addr, prefix);

SELECT '# Check with dense table';

DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data (cidr String) ENGINE = Memory;

INSERT INTO test_data
SELECT
    IPv4NumToString(IPv4CIDRToRange(IPv4StringToNum('255.255.255.255'), toUInt8(number)).1) || '/' || toString(number) AS cidr
FROM system.numbers LIMIT 33;

SELECT sum(isIPAddressInRange('0.0.0.0', cidr)) == 1 FROM test_data;
SELECT sum(isIPAddressInRange('127.0.0.0', cidr)) == 1 FROM test_data;
SELECT sum(isIPAddressInRange('128.0.0.0', cidr)) == 2 FROM test_data;
SELECT sum(isIPAddressInRange('255.0.0.0', cidr)) == 9 FROM test_data;
SELECT sum(isIPAddressInRange('255.0.0.1', cidr)) == 9 FROM test_data;
SELECT sum(isIPAddressInRange('255.0.0.255', cidr)) == 9 FROM test_data;
SELECT sum(isIPAddressInRange('255.255.255.255', cidr)) == 33 FROM test_data;
SELECT sum(isIPAddressInRange('255.255.255.254', cidr)) == 32 FROM test_data;

DROP TABLE IF EXISTS test_data;

SELECT '# Mismatching IP versions is not an error.';

SELECT isIPAddressInRange('127.0.0.1', 'ffff::/16');
SELECT isIPAddressInRange('127.0.0.1', '::127.0.0.1/128');
SELECT isIPAddressInRange('::1', '127.0.0.0/8');
SELECT isIPAddressInRange('::127.0.0.1', '127.0.0.1/32');

SELECT '# Unparsable arguments';

SELECT isIPAddressInRange('unparsable', '127.0.0.0/8'); -- { serverError CANNOT_PARSE_TEXT }
SELECT isIPAddressInRange('127.0.0.1', 'unparsable'); -- { serverError CANNOT_PARSE_TEXT }

SELECT '# Wrong argument types';

SELECT isIPAddressInRange(100, '127.0.0.0/8'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isIPAddressInRange(NULL, '127.0.0.0/8'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isIPAddressInRange(CAST(NULL, 'Nullable(String)'), '127.0.0.0/8'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isIPAddressInRange('127.0.0.1', 100); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isIPAddressInRange(100, NULL); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
WITH arrayJoin([NULL, NULL, NULL, NULL]) AS prefix SELECT isIPAddressInRange([NULL, NULL, 0, 255, 0], prefix); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

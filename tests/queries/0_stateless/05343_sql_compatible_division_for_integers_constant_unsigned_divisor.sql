SET sql_compatible_division_for_integers = 1;
SET enable_parallel_replicas = 0;

CREATE TABLE exact_division_constant_key (x Int64) ENGINE = MergeTree ORDER BY x;
INSERT INTO exact_division_constant_key SELECT toInt64(number) - 50 FROM numbers(100);
-- Safe same-width and wider unsigned constants retain primary-key monotonicity.
SELECT groupArray(x) FROM (SELECT x FROM exact_division_constant_key WHERE x / toUInt64(10) = -3 ORDER BY x) SETTINGS force_primary_key = 1;
SELECT groupArray(x) FROM (SELECT x FROM exact_division_constant_key WHERE x / toUInt128(10) = -3 ORDER BY x) SETTINGS force_primary_key = 1;
SELECT groupArray(x) FROM (SELECT x FROM exact_division_constant_key WHERE x / toUInt256(10) = -3 ORDER BY x) SETTINGS force_primary_key = 1;
SELECT x / toUInt64(10) AS q FROM exact_division_constant_key ORDER BY q DESC LIMIT 3 SETTINGS optimize_read_in_order = 1;
SELECT x / toUInt64(10) AS q FROM exact_division_constant_key ORDER BY q DESC LIMIT 3 SETTINGS optimize_read_in_order = 0;
DROP TABLE exact_division_constant_key;

-- Exercise the signed-cast boundary at every integer width with vector dividends.
SELECT groupArray(toString(x / toUInt8('127'))) FROM values('x Int8', ('-128'), ('-1'), ('0'), ('127'));
SELECT groupArray(toString(x / toUInt8('128'))) FROM values('x Int8', ('-128'), ('-1'), ('0'), ('127'));
SELECT groupArray(toString(x / toUInt8('255'))) FROM values('x Int8', ('-128'), ('-1'), ('0'), ('127'));
SELECT groupArray(toString(x / toUInt16('32767'))) FROM values('x Int16', ('-32768'), ('-1'), ('0'), ('32767'));
SELECT groupArray(toString(x / toUInt16('32768'))) FROM values('x Int16', ('-32768'), ('-1'), ('0'), ('32767'));
SELECT groupArray(toString(x / toUInt16('65535'))) FROM values('x Int16', ('-32768'), ('-1'), ('0'), ('32767'));
SELECT groupArray(toString(x / toUInt32('2147483647'))) FROM values('x Int32', ('-2147483648'), ('-1'), ('0'), ('2147483647'));
SELECT groupArray(toString(x / toUInt32('2147483648'))) FROM values('x Int32', ('-2147483648'), ('-1'), ('0'), ('2147483647'));
SELECT groupArray(toString(x / toUInt32('4294967295'))) FROM values('x Int32', ('-2147483648'), ('-1'), ('0'), ('2147483647'));
SELECT groupArray(toString(x / toUInt64('9223372036854775807'))) FROM values('x Int64', ('-9223372036854775808'), ('-1'), ('0'), ('9223372036854775807'));
SELECT groupArray(toString(x / toUInt64('9223372036854775808'))) FROM values('x Int64', ('-9223372036854775808'), ('-1'), ('0'), ('9223372036854775807'));
SELECT groupArray(toString(x / toUInt64('18446744073709551615'))) FROM values('x Int64', ('-9223372036854775808'), ('-1'), ('0'), ('9223372036854775807'));
SELECT groupArray(toString(x / toUInt128('170141183460469231731687303715884105727'))) FROM values('x Int128', ('-170141183460469231731687303715884105728'), ('-1'), ('0'), ('170141183460469231731687303715884105727'));
SELECT groupArray(toString(x / toUInt128('170141183460469231731687303715884105728'))) FROM values('x Int128', ('-170141183460469231731687303715884105728'), ('-1'), ('0'), ('170141183460469231731687303715884105727'));
SELECT groupArray(toString(x / toUInt128('340282366920938463463374607431768211455'))) FROM values('x Int128', ('-170141183460469231731687303715884105728'), ('-1'), ('0'), ('170141183460469231731687303715884105727'));
SELECT groupArray(toString(x / toUInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967'))) FROM values('x Int256', ('-57896044618658097711785492504343953926634992332820282019728792003956564819968'), ('-1'), ('0'), ('57896044618658097711785492504343953926634992332820282019728792003956564819967'));
SELECT groupArray(toString(x / toUInt256('57896044618658097711785492504343953926634992332820282019728792003956564819968'))) FROM values('x Int256', ('-57896044618658097711785492504343953926634992332820282019728792003956564819968'), ('-1'), ('0'), ('57896044618658097711785492504343953926634992332820282019728792003956564819967'));
SELECT groupArray(toString(x / toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935'))) FROM values('x Int256', ('-57896044618658097711785492504343953926634992332820282019728792003956564819968'), ('-1'), ('0'), ('57896044618658097711785492504343953926634992332820282019728792003956564819967'));

-- A zero unsigned constant still uses lazy checked execution.
SELECT if(x = 0, x / toUInt64(0), 0) FROM values('x Int64', (1)) SETTINGS short_circuit_function_evaluation = 'enable';

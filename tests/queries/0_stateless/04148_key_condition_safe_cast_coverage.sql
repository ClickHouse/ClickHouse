-- Coverage for canBeSafelyCast() in DataTypes/Utils.cpp via KeyCondition.
-- Each table uses a different type as the ORDER BY key, and a WHERE predicate
-- on that key triggers the corresponding canBeSafelyCast branch.

DROP TABLE IF EXISTS t_bfloat16_key;
CREATE TABLE t_bfloat16_key (val BFloat16) ENGINE = MergeTree ORDER BY val;
INSERT INTO t_bfloat16_key VALUES (1.0), (2.0), (3.0);
SELECT val FROM t_bfloat16_key WHERE val > toFloat32(1.5) ORDER BY val;
DROP TABLE t_bfloat16_key;

DROP TABLE IF EXISTS t_float32_key;
CREATE TABLE t_float32_key (val Float32) ENGINE = MergeTree ORDER BY val;
INSERT INTO t_float32_key VALUES (1.0), (2.0), (3.0);
SELECT val FROM t_float32_key WHERE val > toFloat64(1.5) ORDER BY val;
DROP TABLE t_float32_key;

DROP TABLE IF EXISTS t_decimal_key;
CREATE TABLE t_decimal_key (val Decimal32(2)) ENGINE = MergeTree ORDER BY val;
INSERT INTO t_decimal_key VALUES (1.50), (2.50), (3.50);
SELECT val FROM t_decimal_key WHERE val > toDecimal64(2.50, 2) ORDER BY val;
DROP TABLE t_decimal_key;

DROP TABLE IF EXISTS t_uuid_key;
CREATE TABLE t_uuid_key (val UUID) ENGINE = MergeTree ORDER BY val;
INSERT INTO t_uuid_key VALUES ('550e8400-e29b-41d4-a716-446655440001'), ('550e8400-e29b-41d4-a716-446655440002');
SELECT val FROM t_uuid_key WHERE val = '550e8400-e29b-41d4-a716-446655440001';
DROP TABLE t_uuid_key;

DROP TABLE IF EXISTS t_ipv4_key;
CREATE TABLE t_ipv4_key (val IPv4) ENGINE = MergeTree ORDER BY val;
INSERT INTO t_ipv4_key VALUES ('192.168.1.1'), ('10.0.0.1');
SELECT val FROM t_ipv4_key WHERE val = toIPv4('192.168.1.1');
DROP TABLE t_ipv4_key;

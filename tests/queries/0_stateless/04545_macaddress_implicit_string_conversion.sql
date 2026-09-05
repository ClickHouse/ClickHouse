-- Conversion from a bare string literal to `MacAddress`, without an explicit `toMacAddress` call.
SET allow_experimental_macaddress_type = 1;

DROP TABLE IF EXISTS t_macaddress_implicit;
CREATE TABLE t_macaddress_implicit
(
    id UInt32,
    mac MacAddress
) ENGINE = Memory;

SELECT 'INSERT VALUES with a string literal';
INSERT INTO t_macaddress_implicit VALUES (1, '00:1a:2b:3c:4d:5e');
INSERT INTO t_macaddress_implicit VALUES (2, '00-1A-2B-3C-4D-5F');
INSERT INTO t_macaddress_implicit VALUES (3, 'ff:ff:ff:ff:ff:ff');
SELECT id, mac FROM t_macaddress_implicit ORDER BY id;

SELECT 'IN with string literals';
SELECT id FROM t_macaddress_implicit WHERE mac IN ('00:1a:2b:3c:4d:5e', 'ff:ff:ff:ff:ff:ff') ORDER BY id;

SELECT 'Equality against a string literal';
SELECT id FROM t_macaddress_implicit WHERE mac = '00:1a:2b:3c:4d:5e';

DROP TABLE t_macaddress_implicit;

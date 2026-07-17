-- Tags: no-fasttest

-- 1. Settings disabled by default: IPv4-looking string is stored as String in shared data
SELECT '1. Default: no IP inference';
DROP TABLE IF EXISTS t;
CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory;
SET input_format_try_infer_ipv4 = 0;
SET input_format_try_infer_ipv6 = 0;
INSERT INTO t FORMAT JSONEachRow {"ip": "192.168.1.1"};
SELECT dynamicType(json.ip) FROM t;
SELECT json.ip FROM t;
SELECT JSONSharedDataPaths(json) FROM t;
DROP TABLE t;

-- 2. IPv4 inference enabled: string matching IPv4 pattern is inferred as IPv4
SELECT '2. IPv4 inference enabled';
DROP TABLE IF EXISTS t;
CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory;
SET input_format_try_infer_ipv4 = 1;
INSERT INTO t FORMAT JSONEachRow {"ip": "192.168.1.1"};
SELECT dynamicType(json.ip) FROM t;
SELECT json.ip FROM t;
SELECT JSONSharedDataPaths(json) FROM t;
SET input_format_try_infer_ipv4 = 0;
DROP TABLE t;

-- 3. IPv6 inference enabled: string matching IPv6 pattern is inferred as IPv6
SELECT '3. IPv6 inference enabled';
DROP TABLE IF EXISTS t;
CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory;
SET input_format_try_infer_ipv6 = 1;
INSERT INTO t FORMAT JSONEachRow {"ip": "2001:db8::1"};
SELECT dynamicType(json.ip) FROM t;
SELECT json.ip FROM t;
SET input_format_try_infer_ipv6 = 0;
DROP TABLE t;

-- 4. Non-IP strings are not affected even when inference is enabled:
--    "1.2.3"     has only 3 octets
--    "256.0.0.1" has an out-of-range octet
--    "not-an-ip" is plainly not an IP address
SELECT '4. Non-IP strings stay String';
DROP TABLE IF EXISTS t;
CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory;
SET input_format_try_infer_ipv4 = 1;
INSERT INTO t FORMAT JSONEachRow {"ip": "1.2.3"}, {"ip": "256.0.0.1"}, {"ip": "not-an-ip"};
SELECT dynamicType(json.ip) FROM t;
SET input_format_try_infer_ipv4 = 0;
DROP TABLE t;

-- 5. Both settings enabled together: each value is inferred to its correct IP type
SELECT '5. Both IPv4 and IPv6 inference';
DROP TABLE IF EXISTS t;
CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory;
SET input_format_try_infer_ipv4 = 1;
SET input_format_try_infer_ipv6 = 1;
INSERT INTO t FORMAT JSONEachRow {"ip": "192.168.1.1"}, {"ip": "2001:db8::1"};
SELECT dynamicType(json.ip), json.ip FROM t;
SET input_format_try_infer_ipv4 = 0;
SET input_format_try_infer_ipv6 = 0;
DROP TABLE t;

-- 6. Typed path declared in the schema takes precedence over inference settings:
--    even with inference disabled the path is IPv4 because it is declared as such
SELECT '6. Typed path takes precedence';
DROP TABLE IF EXISTS t;
CREATE TABLE t (json JSON(ip IPv4)) ENGINE = Memory;
SET input_format_try_infer_ipv4 = 0;
INSERT INTO t FORMAT JSONEachRow {"ip": "10.0.0.1"};
SELECT toTypeName(json.ip) FROM t;
SELECT json.ip FROM t;
DROP TABLE t;

-- 7. IP inference works independently of date/datetime inference settings
SELECT '7. IP inference with dates/datetimes disabled';
DROP TABLE IF EXISTS t;
CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory;
SET input_format_try_infer_dates = 0;
SET input_format_try_infer_datetimes = 0;
SET input_format_try_infer_ipv4 = 1;
INSERT INTO t FORMAT JSONEachRow {"ip": "10.0.0.1"};
SELECT dynamicType(json.ip) FROM t;
SET input_format_try_infer_ipv4 = 0;
SET input_format_try_infer_dates = 1;
SET input_format_try_infer_datetimes = 1;
DROP TABLE t;

-- 8. Pure IPv4 literal is not inferred as IPv6 when only ipv6 inference is enabled
SELECT '8. IPv4 literal not inferred as IPv6';
DROP TABLE IF EXISTS t;
CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory;
SET input_format_try_infer_ipv4 = 0;
SET input_format_try_infer_ipv6 = 1;
INSERT INTO t FORMAT JSONEachRow {"ip": "192.168.1.1"};
SELECT dynamicType(json.ip) FROM t;
SET input_format_try_infer_ipv6 = 0;
DROP TABLE t;

-- 9. JSON dynamic path: first IP string on a new dynamic path should create IPv4 variant
SELECT '9. Dynamic path IPv4 inference';
DROP TABLE IF EXISTS t;
CREATE TABLE t (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
SET input_format_try_infer_ipv4 = 1;
INSERT INTO t FORMAT JSONEachRow {"ip": "192.168.1.1"};
SELECT dynamicType(json.ip), json.ip FROM t;
SET input_format_try_infer_ipv4 = 0;
DROP TABLE t;

-- 10. JSON dynamic path: nested array of IP strings through shared-data fallback should become Array(IPv4)
SELECT '10. Dynamic path Array(IPv4) inference';
DROP TABLE IF EXISTS t;
CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory;
SET input_format_try_infer_ipv4 = 1;
INSERT INTO t FORMAT JSONEachRow {"ips": ["192.168.1.1", "10.0.0.1"]};
SELECT dynamicType(json.ips), json.ips FROM t;
SET input_format_try_infer_ipv4 = 0;
DROP TABLE t;

-- 11. CSV IPv4 inference (unquoted field)
SELECT '11. CSV IPv4 inference';
SET input_format_try_infer_ipv4 = 1;
desc format(CSV, '192.168.1.1\n');
SET input_format_try_infer_ipv4 = 0;

-- 12. CSV IPv6 inference (unquoted field)
SELECT '12. CSV IPv6 inference';
SET input_format_try_infer_ipv6 = 1;
desc format(CSV, '2001:db8::1\n');
SET input_format_try_infer_ipv6 = 0;

-- 13. TSV IPv4 inference
SELECT '13. TSV IPv4 inference';
SET input_format_try_infer_ipv4 = 1;
desc format(TSV, '192.168.1.1\n');
SET input_format_try_infer_ipv4 = 0;

-- 14. Mixed IPv4 and String collapses to String
SELECT '14. Mixed IPv4 and String collapses to String';
SET input_format_try_infer_ipv4 = 1;
DESCRIBE format(JSONEachRow, '{"ip":"192.168.1.1"}\n{"ip":"not-an-ip"}');
SET input_format_try_infer_ipv4 = 0;

-- 15. Mixed IPv4 and IPv6 collapses to String
SELECT '15. Mixed IPv4 and IPv6 collapses to String';
SET input_format_try_infer_ipv4 = 1;
SET input_format_try_infer_ipv6 = 1;
DESCRIBE format(JSONEachRow, '{"ip":"192.168.1.1"}\n{"ip":"2001:db8::1"}');
SET input_format_try_infer_ipv4 = 0;
SET input_format_try_infer_ipv6 = 0;

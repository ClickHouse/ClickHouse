-- Tags: no-fasttest
-- H3 and JSON functions are not available in the fast test build.
-- https://github.com/ClickHouse/ClickHouse/issues/117205
-- A `LowCardinality` dictionary always holds the type's default value at index 0, even when no row
-- references it. Functions claiming `canBeExecutedOnDefaultArguments` are executed on the whole
-- dictionary, so a function that throws on the default value threw on entirely valid data.

DROP TABLE IF EXISTS t_lc_ip;
CREATE TABLE t_lc_ip (ip LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_ip SELECT concat('10.0.0.', toString(number % 250 + 1)) FROM numbers(1000);
SELECT count(), max(IPv4StringToNum(ip)) FROM t_lc_ip;
SELECT count(), countIf(isIPAddressInRange(ip, '10.0.0.0/8')) FROM t_lc_ip;
DROP TABLE t_lc_ip;

SELECT 'the whole family over a LowCardinality argument';
SELECT IPv4StringToNum(materialize(toLowCardinality('10.0.0.1')));
SELECT hex(IPv6StringToNum(materialize(toLowCardinality('::1'))));
SELECT isIPAddressInRange(materialize(toLowCardinality('10.0.0.1')), '10.0.0.0/8');
SELECT parseDateTime(materialize(toLowCardinality('2024-01-01 00:00:00')), '%Y-%m-%d %H:%i:%s');
SELECT parseDateTime64(materialize(toLowCardinality('2024-01-01 00:00:00')), '%Y-%m-%d %H:%i:%s');
SELECT parseDateTimeInJodaSyntax(materialize(toLowCardinality('2024-01-01 00:00:00')), 'yyyy-MM-dd HH:mm:ss');
SELECT parseTimeDelta(materialize(toLowCardinality('1 min')));
SELECT parseReadableSize(materialize(toLowCardinality('1 KiB')));
SELECT toModifiedJulianDay(materialize(toLowCardinality('2024-01-01')));
SELECT JSONMergePatch(materialize(toLowCardinality('{"a":1}')), '{"b":2}');
SELECT stringToH3(materialize(toLowCardinality('85283473fffffff')));
SELECT h3ToParent(materialize(toLowCardinality(toUInt64(599686042433355775))), 1);
SELECT h3GetResolution(materialize(toLowCardinality(toUInt64(599686042433355775))));
SELECT h3IsPentagon(materialize(toLowCardinality(toUInt64(599686042433355775))));
SELECT h3GetBaseCell(materialize(toLowCardinality(toUInt64(599686042433355775))));
SELECT h3ToCenterChild(materialize(toLowCardinality(toUInt64(599686042433355775))), 6);
SELECT round(h3CellAreaM2(materialize(toLowCardinality(toUInt64(599686042433355775)))));

SELECT 'the results agree with the plain argument type';
SELECT IPv4StringToNum(materialize(toLowCardinality('10.0.0.1'))) = IPv4StringToNum(materialize('10.0.0.1'));
SELECT parseDateTime(materialize(toLowCardinality('2024-01-01 00:00:00')), '%Y-%m-%d %H:%i:%s') = parseDateTime(materialize('2024-01-01 00:00:00'), '%Y-%m-%d %H:%i:%s');
SELECT h3GetResolution(materialize(toLowCardinality(toUInt64(599686042433355775)))) = h3GetResolution(materialize(toUInt64(599686042433355775)));

SELECT 'invalid values are still reported';
SELECT IPv4StringToNum(materialize(toLowCardinality('not an ip'))); -- { serverError CANNOT_PARSE_IPV4 }
SELECT parseDateTime(materialize(toLowCardinality('nonsense')), '%Y-%m-%d %H:%i:%s'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT h3ToParent(materialize(toLowCardinality(toUInt64(1))), 1); -- { serverError INCORRECT_DATA }
SELECT IPv4StringToNumOrNull(materialize(toLowCardinality('not an ip')));
SELECT h3ToParent(materialize(toLowCardinality(toUInt64(1))), 1) SETTINGS functions_h3_default_if_invalid = 1;

SELECT 'a referenced default value is still processed';
SELECT IPv4StringToNumOrNull(materialize(toLowCardinality(''))) IS NULL;
SELECT count() FROM (SELECT IPv4StringToNumOrDefault(toLowCardinality(arrayJoin(['', '10.0.0.1']))));

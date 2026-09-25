-- Tags: no-fasttest
-- no-fasttest: RapidJSON, S2 and OpenSSL functions are not available in the fast test build.
-- Random settings limits: short_circuit_function_evaluation_for_nulls_threshold=(1, None)
-- A function that rejects the default value of its argument type (an empty string, zero, the zero
-- time) must still accept a LowCardinality argument, whose dictionary always holds that value, and a
-- Nullable argument, which holds it behind every NULL. Only the values the rows hold are evaluated.
-- https://github.com/ClickHouse/ClickHouse/issues/122277
-- https://github.com/ClickHouse/ClickHouse/issues/121498

DROP TABLE IF EXISTS t_default_arguments;
CREATE TABLE t_default_arguments (s Nullable(String), lc LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_default_arguments VALUES ('{"a":1}', '{"b":2}'), (NULL, '{"c":3}');
SELECT prettyPrintJSON(s), prettyPrintJSON(lc) FROM t_default_arguments ORDER BY ALL;
DROP TABLE t_default_arguments;

SELECT 'LowCardinality';
SELECT printf(materialize(toLowCardinality('%d')), 1);
SELECT formatQuery(materialize(toLowCardinality('select 1')));
SELECT formatQuerySingleLine(materialize(toLowCardinality('select 1')));
SELECT parseQueryToJSON(materialize(toLowCardinality('select 1'))) = parseQueryToJSON('select 1');
SELECT formatQueryFromJSON(materialize(toLowCardinality(parseQueryToJSON('select 1'))));
SELECT conv(materialize(toLowCardinality('10')), 10, 2);
SELECT toBool(materialize(toLowCardinality('true')));
SELECT filesystemCapacity(materialize(toLowCardinality('default'))) > 0;
SELECT base64Decode(materialize(toLowCardinality(toFixedString('SGVsbG8=', 8))));
SELECT hex(encrypt('aes-256-ecb', 'text', materialize(toLowCardinality('12345678901234567890123456789012'))));
SELECT clamp(5, 1, toUInt8(materialize(toLowCardinality('10'))));
SELECT hopStart(toDateTime(materialize(toLowCardinality('2026-01-02 03:04:05')), 'UTC'), INTERVAL 1 DAY, INTERVAL 2 DAY);

SELECT 'Nullable';
SELECT printf(x, 1) FROM (SELECT arrayJoin(['%d', NULL]) AS x) ORDER BY ALL;
SELECT formatQuerySingleLine(x) FROM (SELECT arrayJoin(['select 1', NULL]) AS x) ORDER BY ALL;
SELECT parseQueryToJSON(x) = parseQueryToJSON('select 1') FROM (SELECT arrayJoin(['select 1', NULL]) AS x) ORDER BY ALL;
SELECT formatQueryFromJSON(x) FROM (SELECT arrayJoin([parseQueryToJSON('select 1'), NULL]) AS x) ORDER BY ALL;
SELECT conv(x, 10, 2) FROM (SELECT arrayJoin(['10', NULL]) AS x) ORDER BY ALL;
SELECT conv('10', 10, x) FROM (SELECT arrayJoin([2, NULL]) AS x) ORDER BY ALL;
SELECT MGRSToGeo(x) FROM (SELECT arrayJoin(['31UDQ4825111935', NULL]) AS x) ORDER BY ALL;
SELECT readWKTPoint(x) FROM (SELECT arrayJoin(['POINT (1 2)', NULL]) AS x) ORDER BY ALL;
SELECT readWKBPoint(x) FROM (SELECT arrayJoin([unhex('0101000000000000000000f03f0000000000000040'), NULL]) AS x) ORDER BY ALL;
SELECT filesystemCapacity(x) > 0 FROM (SELECT arrayJoin(['default', NULL]) AS x) ORDER BY ALL;
SELECT base58Decode(x, 7) FROM (SELECT arrayJoin(['3dc8KtHrwM', NULL]) AS x) ORDER BY ALL;
SELECT decrypt('aes-256-ecb', encrypt('aes-256-ecb', 'text', '12345678901234567890123456789012'), x) FROM (SELECT arrayJoin(['12345678901234567890123456789012', NULL]) AS x) ORDER BY ALL;
SELECT divideDecimal(toDecimal32(12, 1), x) FROM (SELECT arrayJoin([toDecimal32(2, 1), NULL]) AS x) ORDER BY ALL;
SELECT clamp(5, 1, x), widthBucket(10.15, -8.6, 23, x), regexpPosition('aXbXcXd', 'X', x) FROM (SELECT arrayJoin([4, NULL]) AS x) ORDER BY ALL;
SELECT tupleIntDivByNumber((15, 10), x), tupleModuloByNumber((15, 10), x), tuplePositiveModuloByNumber((15, 10), x) FROM (SELECT arrayJoin([4, NULL]) AS x) ORDER BY ALL;
SELECT tupleIntDiv((15, 10), x), tupleModulo((15, 10), x) FROM (SELECT arrayJoin([(4, 3), NULL]) AS x) ORDER BY ALL;
SELECT tupleDivideByNumber((toDecimal32(15, 1), toDecimal32(10, 1)), x) FROM (SELECT arrayJoin([toDecimal32(2, 1), NULL]) AS x) ORDER BY ALL;
SELECT hop(x, INTERVAL 1 DAY, INTERVAL 2 DAY), hopStart(x, INTERVAL 1 DAY, INTERVAL 2 DAY), hopEnd(x, INTERVAL 1 DAY, INTERVAL 2 DAY) FROM (SELECT arrayJoin([toDateTime('2026-01-02 03:04:05', 'UTC'), NULL]) AS x) ORDER BY ALL;
SELECT s2ToGeo(x) FROM (SELECT arrayJoin([toUInt64(4704772434919038107), NULL]) AS x) ORDER BY ALL;
SELECT s2CapUnion(x, 1.0, 1157347770437378819, 1.0) FROM (SELECT arrayJoin([toUInt64(1157339245694594829), NULL]) AS x) ORDER BY ALL;
SELECT s2RectAdd(5765131099823669248, x, 5765131099956887552) FROM (SELECT arrayJoin([toUInt64(5765131099823669248), NULL]) AS x) ORDER BY ALL;
SELECT s2RectUnion(5178914411069187297, x, 5179062030687166815, 5177056748191934217), s2RectIntersection(5178914411069187297, x, 5179062030687166815, 5177056748191934217) FROM (SELECT arrayJoin([toUInt64(5177056748191934217), NULL]) AS x) ORDER BY ALL;

SELECT 'invalid values are still reported';
SELECT prettyPrintJSON(materialize(toLowCardinality('not json'))); -- { serverError BAD_ARGUMENTS }
SELECT printf(materialize(toLowCardinality('%d %d')), 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT formatQuery(materialize(toLowCardinality(''))); -- { serverError SYNTAX_ERROR }
SELECT conv(x, 10, 2) FROM (SELECT arrayJoin(['', NULL]) AS x); -- { serverError BAD_ARGUMENTS }
SELECT hopStart(x, INTERVAL 1 DAY, INTERVAL 2 DAY) FROM (SELECT arrayJoin([toDateTime(0, 'UTC'), NULL]) AS x); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryOrNull(materialize(toLowCardinality('select (')));

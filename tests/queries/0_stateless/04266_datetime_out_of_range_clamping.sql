-- Tags: no-fasttest
-- Test that DateTime values are clamped (not wrapped) for out-of-range inputs.
-- See https://github.com/ClickHouse/ClickHouse/issues/103094

SET session_timezone = 'UTC';

SELECT 'JSONExtract';
SELECT
    JSONExtract('{"d":"1960-01-01 00:00:00"}', 'd', 'DateTime') AS pre_epoch,
    JSONExtract('{"d":"2200-01-01 00:00:00"}', 'd', 'DateTime') AS post_max;

SELECT 'JSONExtract numeric';
SELECT
    JSONExtract('{"d":0}', 'd', 'DateTime') AS zero,
    JSONExtract('{"d":4294967295}', 'd', 'DateTime') AS max_uint32,
    JSONExtract('{"d":4294967296}', 'd', 'DateTime') AS over_uint32,
    JSONExtract('{"d":18446744073709551615}', 'd', 'DateTime') AS max_uint64;

SELECT 'JSONExtract numeric negative';
SELECT JSONExtract('{"d":-1}', 'd', 'DateTime');

SELECT 'JSONExtract numeric Int64 large';
SELECT JSONExtract('{"d":9223372036854775807}', 'd', 'DateTime');

SELECT 'JSONExtractKeysAndValues';
SELECT JSONExtractKeysAndValues('{"pre":"1960-01-01 00:00:00","post":"2200-01-01 00:00:00"}', 'DateTime');

SELECT 'CSV';
SELECT d FROM format(CSV, 'd DateTime', '"1960-01-01 00:00:00"');
SELECT d FROM format(CSV, 'd DateTime', '"2200-01-01 00:00:00"');

SELECT 'TSV';
SELECT d FROM format(TSV, 'd DateTime', '1960-01-01 00:00:00');
SELECT d FROM format(TSV, 'd DateTime', '2200-01-01 00:00:00');

SELECT 'JSONEachRow';
SELECT d FROM format(JSONEachRow, 'd DateTime', '{"d":"1960-01-01 00:00:00"}');
SELECT d FROM format(JSONEachRow, 'd DateTime', '{"d":"2200-01-01 00:00:00"}');

SELECT 'JSONEachRow numeric';
SELECT d FROM format(JSONEachRow, 'd DateTime', '{"d":0}');
SELECT d FROM format(JSONEachRow, 'd DateTime', '{"d":4294967295}');
SELECT d FROM format(JSONEachRow, 'd DateTime', '{"d":4294967296}');
SELECT d FROM format(JSONEachRow, 'd DateTime', '{"d":-1}');

SELECT 'Values';
SELECT d FROM format(Values, 'd DateTime', '(\'1960-01-01 00:00:00\')');
SELECT d FROM format(Values, 'd DateTime', '(\'2200-01-01 00:00:00\')');

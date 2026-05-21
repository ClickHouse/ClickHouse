-- Tags: no-fasttest
-- Test that DateTime values are clamped (not wrapped) for out-of-range inputs.
-- See https://github.com/ClickHouse/ClickHouse/issues/103094

SET session_timezone = 'UTC';

SELECT 'JSONExtract';
SELECT
    JSONExtract('{"d":"1960-01-01 00:00:00"}', 'd', 'DateTime') AS pre_epoch,
    JSONExtract('{"d":"2200-01-01 00:00:00"}', 'd', 'DateTime') AS post_max;

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

SELECT 'Values';
SELECT d FROM format(Values, 'd DateTime', '(\'1960-01-01 00:00:00\')');
SELECT d FROM format(Values, 'd DateTime', '(\'2200-01-01 00:00:00\')');

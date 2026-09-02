-- Regression test for https://github.com/ClickHouse/ClickHouse/pull/80263
-- `IPv4StringToNum*` / `IPv6StringToNum*` take over the `LowCardinality` execution path themselves
-- (`useDefaultImplementationForLowCardinalityColumns` returns `false`), so they must preserve the
-- `LowCardinality` output contract: a `LowCardinality` argument must return a `LowCardinality` result
-- (as before PR #80263), not a materialized plain result.

SELECT 'Types: LowCardinality(String) input';
SELECT toTypeName(IPv4StringToNum(toLowCardinality('1.2.3.4')));
SELECT toTypeName(IPv6StringToNum(toLowCardinality('::1')));
SELECT toTypeName(IPv4StringToNumOrDefault(toLowCardinality('1.2.3.4')));
SELECT toTypeName(IPv6StringToNumOrDefault(toLowCardinality('::1')));
SELECT toTypeName(IPv4StringToNumOrNull(toLowCardinality('1.2.3.4')));
SELECT toTypeName(IPv6StringToNumOrNull(toLowCardinality('::1')));

SELECT 'Types: LowCardinality(Nullable(String)) input';
SELECT toTypeName(IPv4StringToNum(CAST(materialize('1.2.3.4') AS LowCardinality(Nullable(String)))));
SELECT toTypeName(IPv6StringToNum(CAST(materialize('::1') AS LowCardinality(Nullable(String)))));

SELECT 'Non-LowCardinality input stays non-LowCardinality';
SELECT toTypeName(IPv4StringToNum('1.2.3.4'));
SELECT toTypeName(IPv6StringToNumOrNull(materialize('::1')));

SELECT 'Values match the plain path';
SELECT IPv4StringToNum(toLowCardinality('1.2.3.4')) = IPv4StringToNum('1.2.3.4');
SELECT hex(IPv6StringToNum(toLowCardinality('::1'))) = hex(IPv6StringToNum('::1'));
SELECT IPv4StringToNumOrNull(toLowCardinality('bad')) IS NULL;
SELECT IPv6StringToNumOrNull(toLowCardinality('bad')) IS NULL;
SELECT IPv4StringToNumOrDefault(toLowCardinality('bad')) = IPv4StringToNumOrDefault('bad');

SELECT 'Over many rows through the fast path';
SELECT DISTINCT toTypeName(IPv4StringToNum(toLowCardinality(v))) FROM (SELECT arrayJoin(['1.2.3.4', '5.6.7.8', '1.2.3.4']) AS v);
SELECT groupArray(IPv4StringToNum(toLowCardinality(v))) FROM (SELECT arrayJoin(['0.0.0.1', '0.0.0.2', '0.0.0.1']) AS v);

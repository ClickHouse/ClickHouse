-- Tags: stateful
-- The test uses quite a bit of memory. A low max_bytes_before_external_group_by value will lead to high disk usage
-- which in CI leads to timeouts
SET max_bytes_before_external_group_by=0;
SET max_bytes_ratio_before_external_group_by=0;
SELECT CounterID AS k, quantileExact(0.5)(ResolutionWidth) FROM test.hits GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;
SELECT CounterID AS k, quantilesExact(0.1, 0.5, 0.9, 0.99, 0.999)(ResolutionWidth) FROM test.hits GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;

SELECT CounterID AS k, quantileTiming(0.5)(ResolutionWidth) FROM test.hits GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;
SELECT CounterID AS k, quantilesTiming(0.1, 0.5, 0.9, 0.99, 0.999)(ResolutionWidth) FROM test.hits GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;


SELECT CounterID AS k, quantileExact(0.5)(ResolutionWidth) FROM remote('127.0.0.{1,2}', test.hits) GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;
SELECT CounterID AS k, quantilesExact(0.1, 0.5, 0.9, 0.99, 0.999)(ResolutionWidth) FROM remote('127.0.0.{1,2}', test.hits) GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;

SELECT CounterID AS k, quantileTiming(0.5)(ResolutionWidth) FROM remote('127.0.0.{1,2}', test.hits) GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;
SELECT CounterID AS k, quantilesTiming(0.1, 0.5, 0.9, 0.99, 0.999)(ResolutionWidth) FROM remote('127.0.0.{1,2}', test.hits) GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;

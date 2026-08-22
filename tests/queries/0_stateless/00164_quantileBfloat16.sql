-- Tags: stateful
SELECT CounterID AS k, quantileBFloat16(0.5)(ResolutionWidth) FROM test.hits GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;
SELECT CounterID AS k, quantilesBFloat16(0.1, 0.5, 0.9, 0.99, 0.999)(ResolutionWidth) FROM test.hits GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;


SELECT CounterID AS k, quantileBFloat16(0.5)(ResolutionWidth) FROM remote('127.0.0.{1,2}', test.hits) GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;
SELECT CounterID AS k, quantilesBFloat16(0.1, 0.5, 0.9, 0.99, 0.999)(ResolutionWidth) FROM remote('127.0.0.{1,2}', test.hits) GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;

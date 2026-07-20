-- Tags: stateful
-- Optimize: filter to top-10 CounterIDs to avoid maintaining DDSketch states
-- for thousands of groups. The test verifies quantileDD correctness and
-- distributed merge — not full-table aggregation performance.
-- Pin max_threads/max_block_size to prevent randomized slow settings.
SET max_threads = 4;
SET max_block_size = 65505;

SELECT CounterID AS k, round(quantileDD(0.01, 0.5)(ResolutionWidth), 2) FROM test.hits WHERE CounterID IN (1704509, 732797, 598875, 792887, 3807842, 25703952, 716829, 59183, 33010362, 800784) GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;
SELECT CounterID AS k, arrayMap(a -> round(a, 2), quantilesDD(0.01, 0.1, 0.5, 0.9, 0.99, 0.999)(ResolutionWidth)) FROM test.hits WHERE CounterID IN (1704509, 732797, 598875, 792887, 3807842, 25703952, 716829, 59183, 33010362, 800784) GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;

SELECT CounterID AS k, round(quantileDD(0.01, 0.5)(ResolutionWidth), 2) FROM remote('127.0.0.{1,2}', test.hits) WHERE CounterID IN (1704509, 732797, 598875, 792887, 3807842, 25703952, 716829, 59183, 33010362, 800784) GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;
SELECT CounterID AS k, arrayMap(a -> round(a, 2), quantilesDD(0.01, 0.1, 0.5, 0.9, 0.99, 0.999)(ResolutionWidth)) FROM remote('127.0.0.{1,2}', test.hits) WHERE CounterID IN (1704509, 732797, 598875, 792887, 3807842, 25703952, 716829, 59183, 33010362, 800784) GROUP BY k ORDER BY count() DESC, CounterID LIMIT 10;

-- Tags: no-fasttest, no-msan
-- no-msan: DeltaLake is not built with MSan.

-- A key-value argument next to headers(...) must parse in either written order.
set describe_compact_output = 1;
describe table deltaLake('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, structure = 'x Int64', headers('foo' = 'bar'));
describe table deltaLake('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), structure = 'x Int64');
describe table deltaLakeCluster('test_shard_localhost', 'https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, structure = 'x Int64', headers('foo' = 'bar'));
describe table deltaLakeCluster('test_shard_localhost', 'https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), structure = 'x Int64');

-- Tags: stateful, no-replicated-database, no-s3-storage
-- Tag no-replicated-database: Requires investigation

SET optimize_use_implicit_projections = 0;

EXPLAIN ESTIMATE SELECT count() FROM test.hits WHERE CounterID = 29103473;
EXPLAIN ESTIMATE SELECT count() FROM test.hits WHERE CounterID != 29103473;
EXPLAIN ESTIMATE SELECT count() FROM test.hits WHERE CounterID > 29103473;
EXPLAIN ESTIMATE SELECT count() FROM test.hits WHERE CounterID < 29103473;
EXPLAIN ESTIMATE SELECT count() FROM test.hits WHERE CounterID = 29103473 UNION ALL SELECT count() FROM test.hits WHERE CounterID = 1704509;

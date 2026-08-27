SET max_threads = 8;

-- This triggered a nullptr dereference due to the confusion between old and new analyzers:
SELECT sum(*) FROM remote('127.0.0.4', currentDatabase(), viewExplain('EXPLAIN PIPELINE', 'graph = 1', (SELECT * FROM remote('127.0.0.4', system, one)))); -- { serverError UNKNOWN_FUNCTION }
SELECT groupArray(*) FROM cluster(test_cluster_two_shards, viewExplain('EXPLAIN PIPELINE', 'graph = 1', (SELECT * FROM remote('127.0.0.4', system, one))));

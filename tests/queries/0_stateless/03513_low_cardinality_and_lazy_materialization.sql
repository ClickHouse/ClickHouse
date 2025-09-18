-- Tags: long, no-debug, no-asan, no-tsan, no-msan, no-ubsan
-- Verify that LowCardinality column is correctly read in Lazy Materialization codepath
DROP TABLE IF EXISTS xx;

CREATE TABLE xx
(
    id Int32,
    v LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 2048;

INSERT INTO xx SELECT rand(), randomPrintableASCII(4) FROM numbers(10000000) SETTINGS max_insert_threads = 1;

OPTIMIZE TABLE xx FINAL;

-- No output from the SELECT, just ensure no exceptions
SELECT id,v FROM xx ORDER BY id DESC LIMIT 100 SETTINGS query_plan_max_limit_for_lazy_materialization=100,query_plan_optimize_lazy_materialization=1 FORMAT null;



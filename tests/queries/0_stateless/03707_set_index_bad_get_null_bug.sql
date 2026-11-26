SET explain_query_plan_default = 'legacy';
set enable_parallel_replicas = 0;

drop table if exists test;
CREATE table test
(
    `ts` Int64,
    `v` LowCardinality(String),
    INDEX v v TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (ts);

INSERT INTO test (v) FORMAT Values ('VALUE1');

EXPLAIN indexes = 1, description=0 SELECT CAST(NULL, 'Nullable(String)') AS source, v AS v FROM test WHERE (source = 'VALUE1') OR (v ILIKE 'VALUE1');

-- The row matches via the second disjunct, so the granule must not be pruned: the set
-- index must return the same rows as a scan with skip indexes disabled.
SELECT count() FROM test WHERE (CAST(NULL, 'Nullable(String)') = 'VALUE1') OR (v ILIKE 'VALUE1');
SELECT count() FROM test WHERE (CAST(NULL, 'Nullable(String)') = 'VALUE1') OR (v ILIKE 'VALUE1') SETTINGS use_skip_indexes = 0;

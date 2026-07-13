CREATE TABLE IF NOT EXISTS repro
(
    `a` LowCardinality(String),
    `foos` Nested(x LowCardinality(String))
)
ENGINE = MergeTree
ORDER BY a;

CREATE TABLE IF NOT EXISTS repro_dist
(
   "a" LowCardinality(String),
   "foos" Nested(
      "x" LowCardinality(String),
   )
) ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 'repro');

SELECT
    a,
    foo.x
FROM repro_dist
ARRAY JOIN foos AS foo;

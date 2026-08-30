-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/79949
-- A `Merge` table over `Distributed` tables reads its children at a stage above `FetchColumns`,
-- so each child sorts on the shards and the step above `ReadFromMerge` only merges already
-- sorted streams. Narrowing the pipe used to destroy that per-stream order, which returned
-- wrongly ordered - and, together with `LIMIT`, incomplete - results.

DROP TABLE IF EXISTS t_negative;
DROP TABLE IF EXISTS t_positive;
DROP TABLE IF EXISTS dist_negative;
DROP TABLE IF EXISTS dist_positive;
DROP TABLE IF EXISTS merge_one;
DROP TABLE IF EXISTS merge_two;

CREATE TABLE t_negative ENGINE = Memory AS SELECT -number AS A FROM numbers(10000);
CREATE TABLE t_positive ENGINE = Memory AS SELECT toInt64(number) AS A FROM numbers(10000);

CREATE TABLE dist_negative AS t_negative
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_negative);
CREATE TABLE dist_positive AS t_positive
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_positive);

CREATE TABLE merge_one AS t_negative ENGINE = Merge(currentDatabase(), '^dist_negative$');
CREATE TABLE merge_two AS t_negative ENGINE = Merge(currentDatabase(), '^dist_(negative|positive)$');

-- A single-table `Merge` reports the stage of its only child, so its shard streams used to be
-- narrowed down to `max_threads` and lost their order. Expected: -4999 -4999 -5000 -5000 -5001.
SELECT A FROM merge_one ORDER BY A DESC LIMIT 9998, 5 SETTINGS max_threads = 1;

-- The original report: a `Merge` over two `Distributed` tables. Expected: 9999 9999 9998 9998 9997.
SELECT A FROM merge_two ORDER BY A DESC LIMIT 5
    SETTINGS max_threads = 1, distributed_aggregation_memory_efficient = 0;

DROP TABLE merge_two;
DROP TABLE merge_one;
DROP TABLE dist_positive;
DROP TABLE dist_negative;
DROP TABLE t_positive;
DROP TABLE t_negative;

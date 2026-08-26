-- Tags: no-random-settings, no-random-merge-tree-settings
-- The abandonment fires only after several read chunks per stream, so the profile-event assertion
-- depends on the stream counts and chunk sizes, which randomized settings (and a randomized
-- `index_granularity`) can reshape.

-- The per-stream pre-deduplication in front of an `IN`-subquery set fill (see
-- `allow_creating_set_partitions_independently`) abandons on mostly-unique input like the preliminary
-- `DISTINCT` does, and the `DistinctTransformsAbandonedDeduplication` profile event records it.

-- The per-partition set build is not applied with parallel replicas, and the query_log check below
-- reads the profile event from the single initiator query.
SET enable_parallel_replicas = 0;

-- Small blocks complete the abandonment observation window early even on the small test table.
SET max_block_size = 512;

-- { echo }

DROP TABLE IF EXISTS t_in_uniq;
-- The per-partition read emits whole granules per chunk, so a small `index_granularity` is what gives
-- each partition stream enough chunks to complete the observation window and abandon.
CREATE TABLE t_in_uniq (a UInt64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY sipHash64(a) % 8 SETTINGS index_granularity = 512;
INSERT INTO t_in_uniq SELECT number FROM numbers(40000);
SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM t_in_uniq) SETTINGS max_threads = 8, log_comment = '05046_in_set';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['DistinctTransformsAbandonedDeduplication'] > 0 FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '05046_in_set' AND type = 'QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1;
DROP TABLE t_in_uniq;

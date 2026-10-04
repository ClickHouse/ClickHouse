-- The local write of an INSERT into a Distributed table runs through nested INSERTs into the
-- underlying table. The `Too many parts` check of those nested INSERTs must not count the parts
-- the same outer query has already committed on the local target: with `parts_to_throw_insert = 1`
-- the second block of a multi-block INSERT used to fail with TOO_MANY_PARTS after the first block
-- had already written a part.

DROP TABLE IF EXISTS t_04826_local;
DROP TABLE IF EXISTS t_04826_dist;

CREATE TABLE t_04826_local (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;

CREATE TABLE t_04826_dist (n UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_04826_local);

-- The background-send path writes the local shard directly (`prefer_localhost_replica`), one
-- nested INSERT per block.
INSERT INTO t_04826_dist SELECT number FROM numbers(2)
    SETTINGS distributed_foreground_insert = 0, prefer_localhost_replica = 1,
        max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
        max_insert_threads = 1;

SELECT count() FROM t_04826_local;

TRUNCATE TABLE t_04826_local;

-- The foreground path keeps one nested INSERT per local replica job, but the outer query may fan
-- out into several sinks, each with its own job for the local target.
INSERT INTO t_04826_dist SELECT number FROM numbers(4)
    SETTINGS distributed_foreground_insert = 1, prefer_localhost_replica = 1,
        max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
        max_insert_threads = 2;

SELECT count() FROM t_04826_local;

DROP TABLE t_04826_dist;
DROP TABLE t_04826_local;

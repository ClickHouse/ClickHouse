-- Parallel right-side fill of a spilling `HashJoin` must not throw
-- `LOGICAL_ERROR` from `HashJoin::addBlockToJoin` when `GraceHashJoin`'s
-- in-memory join is filled from several pipeline threads. Inserts are
-- serialized under `hash_join_mutex`, but each fill stream still owns a
-- worker slot passed as `worker_id`.

SET max_threads = 8;
SET max_block_size = 8192;
SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 16777216;
SET max_bytes_ratio_before_external_join = 0;
SET grace_hash_join_initial_buckets = 1;

SELECT count()
FROM (SELECT number AS k FROM numbers(2000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(2000000)) AS t2
USING (k);

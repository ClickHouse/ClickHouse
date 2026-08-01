-- Transposed distance functions over a QBit with a constant reference vector under parallel replicas.
-- `DistanceTransposedPartialReadsPass` wraps the reference vector in `_CAST(..., 'Array(BFloat16)')` after
-- resolution. When the reference is a constant, the initiator would keep a live `_CAST(<constant>, <type>)`
-- named after its source expression, while a remote replica receives the constant serialized as a plain
-- literal (the `_CAST` source expression is dropped on serialization), folds it, and names it by value. The
-- pass must therefore fold the `_CAST` on the initiator so that both sides name the constant identically,
-- otherwise the initiator cannot find the expected column in blocks received from remote replicas.
-- https://github.com/ClickHouse/ClickHouse/issues/110719

SET allow_experimental_qbit_type = 1;

DROP TABLE IF EXISTS qbit_pr_const;

CREATE TABLE qbit_pr_const
(
    id UInt32,
    v  Array(Float32),
    qb QBit(BFloat16, 32, 16) DEFAULT CAST(v, 'Array(BFloat16)')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO qbit_pr_const (id, v) SELECT number, arrayMap(i -> toFloat32(i + number), range(32)) FROM numbers(1000);

-- A plain literal reference vector.
SELECT id FROM qbit_pr_const
ORDER BY cosineDistanceTransposed(qb, [toFloat32(0), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31], 16) ASC, id ASC
LIMIT 3;

-- A plain literal reference vector with parallel replicas, reading from remote replicas only.
SELECT id FROM qbit_pr_const
ORDER BY cosineDistanceTransposed(qb, [toFloat32(0), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31], 16) ASC, id ASC
LIMIT 3
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0,
         parallel_replicas_min_number_of_rows_per_replica = 0;

-- A `WITH`-bound constant reference vector with parallel replicas.
WITH arrayMap(i -> toFloat32(i), range(32)) AS ref
SELECT id FROM qbit_pr_const
ORDER BY cosineDistanceTransposed(qb, ref, 16) ASC, id ASC
LIMIT 3
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0,
         parallel_replicas_min_number_of_rows_per_replica = 0;

DROP TABLE qbit_pr_const;

-- A constant reference vector whose serialized name exceeds `optimize_const_name_size` (256, the default).
-- Such an over-threshold constant is rewritten by `buildQueryTreeForShard`'s `ReplaceLongConstWithScalarVisitor`
-- into `__getScalar('<hash>')` before the query is shipped to a replica, so the reference must still be named
-- identically on the initiator and the replica -- otherwise the initiator cannot find the column in blocks
-- received from remote replicas (the same NOT_FOUND_COLUMN_IN_BLOCK class as above, raised in review of #110729).
-- A 128-element reference vector has a name longer than 256 characters.
DROP TABLE IF EXISTS qbit_pr_const_large;

CREATE TABLE qbit_pr_const_large
(
    id UInt32,
    v  Array(Float32),
    qb QBit(BFloat16, 128, 16) DEFAULT CAST(v, 'Array(BFloat16)')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO qbit_pr_const_large (id, v) SELECT number, arrayMap(i -> toFloat32(i + number), range(128)) FROM numbers(1000);

-- Without parallel replicas.
WITH arrayMap(i -> toFloat32(i), range(128)) AS ref
SELECT id FROM qbit_pr_const_large
ORDER BY cosineDistanceTransposed(qb, ref, 16) ASC, id ASC
LIMIT 3;

-- With parallel replicas, reading from remote replicas only.
WITH arrayMap(i -> toFloat32(i), range(128)) AS ref
SELECT id FROM qbit_pr_const_large
ORDER BY cosineDistanceTransposed(qb, ref, 16) ASC, id ASC
LIMIT 3
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0,
         parallel_replicas_min_number_of_rows_per_replica = 0;

DROP TABLE qbit_pr_const_large;

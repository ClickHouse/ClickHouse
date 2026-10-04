-- Transposed distance functions over a QBit with a scalar-subquery reference vector under parallel replicas.
-- The scalar subquery becomes `__getScalar('<hash>')` on the initiator and `DistanceTransposedPartialReadsPass`
-- wraps it in `_CAST(..., 'Array(BFloat16)')` after resolution. On a remote replica the received
-- `_CAST(__getScalar(...), ...)` expression is constant-folded, and the folded constant must be named after its
-- source expression so that the initiator finds the expected column in blocks received from remote replicas.
-- https://github.com/ClickHouse/ClickHouse/issues/110719

SET allow_experimental_qbit_type = 1;

DROP TABLE IF EXISTS qbit_pr;

CREATE TABLE qbit_pr
(
    id UInt32,
    v  Array(Float32),
    qb QBit(BFloat16, 32, 16) DEFAULT CAST(v, 'Array(BFloat16)')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO qbit_pr (id, v) SELECT number, arrayMap(i -> toFloat32(i + number), range(32)) FROM numbers(1000);

-- Without parallel replicas
WITH (SELECT CAST(qb, 'Array(Float32)') FROM qbit_pr WHERE id = 0) AS ref
SELECT id FROM qbit_pr ORDER BY cosineDistanceTransposed(qb, ref, 16) ASC, id ASC LIMIT 3;

-- With parallel replicas, reading from remote replicas only
WITH (SELECT CAST(qb, 'Array(Float32)') FROM qbit_pr WHERE id = 0) AS ref
SELECT id FROM qbit_pr ORDER BY cosineDistanceTransposed(qb, ref, 16) ASC, id ASC LIMIT 3
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0,
         parallel_replicas_min_number_of_rows_per_replica = 0;

DROP TABLE qbit_pr;

-- The input-bytes estimate for a compact part serializes a sample of every column it reads and
-- compresses it with the column's `CODEC`. The codec is resolved from the table metadata, which does not
-- describe that sample on its own, and a type-specific codec applied to a stream it was not resolved for
-- used to fail the query: `ALP` rejects a byte count that is not a whole number of floats.

SET enable_alp_codec = 1;

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'parallel_replicas';

-- A column stored `Sparse` reaches the estimate as sparse offsets plus values in one buffer.

DROP TABLE IF EXISTS t_autopr_sparse_codec;

-- A compact part carries no per-column sizes, so the estimate has to serialize a sample; a zero
-- `ratio_of_defaults_for_sparse_serialization` makes every column sparse.
CREATE TABLE t_autopr_sparse_codec
(
    i UInt32,
    f1 Float32 CODEC(ALP(RD)),
    f2 Float32 CODEC(ALP(RD)),
    f3 Float32 CODEC(ALP(RD)),
    f4 Float32 CODEC(ALP(RD))
)
ENGINE = MergeTree ORDER BY i
SETTINGS min_bytes_for_wide_part = '1G', ratio_of_defaults_for_sparse_serialization = 0,
         index_granularity = 8192, index_granularity_bytes = '1G';

-- The four columns differ only in how many of the leading rows are default, so their sparse samples have
-- four consecutive lengths: whatever the sampled row count is, three of them are not a whole number of
-- floats.
INSERT INTO t_autopr_sparse_codec SELECT
    number,
    if(number % 3 = 0, toFloat32(0), toFloat32(number * 0.125)),
    if(number % 3 = 0 AND number > 0, toFloat32(0), toFloat32(number * 0.125)),
    if(number % 3 = 0 AND number > 3, toFloat32(0), toFloat32(number * 0.125)),
    if(number % 3 = 0 AND number > 6, toFloat32(0), toFloat32(number * 0.125))
FROM numbers(5000);

SELECT sum(f1), sum(f2), sum(f3), sum(f4) FROM t_autopr_sparse_codec;

DROP TABLE t_autopr_sparse_codec;

-- A column read from a part keeps its pre-`ALTER` type until the mutation that rewrites it has run, so
-- the codec resolved from the metadata type does not describe the sample either.

DROP TABLE IF EXISTS t_autopr_altered_codec;

CREATE TABLE t_autopr_altered_codec (i UInt32, f Float32 CODEC(ALP(RD)))
ENGINE = MergeTree ORDER BY i
SETTINGS min_bytes_for_wide_part = '1G', ratio_of_defaults_for_sparse_serialization = 1.1,
         index_granularity = 8192, index_granularity_bytes = '1G';

-- An odd row count below the randomized `max_block_size` floor: the whole part is one block and one
-- sample, of `Float32` values that the metadata by then calls `Float64`.
INSERT INTO t_autopr_altered_codec SELECT number, toFloat32(number * 0.125) FROM numbers(5001);

SYSTEM STOP MERGES t_autopr_altered_codec;
ALTER TABLE t_autopr_altered_codec MODIFY COLUMN f Float64 CODEC(ALP(RD)) SETTINGS alter_sync = 0;

SELECT sum(f) FROM t_autopr_altered_codec;

DROP TABLE t_autopr_altered_codec;

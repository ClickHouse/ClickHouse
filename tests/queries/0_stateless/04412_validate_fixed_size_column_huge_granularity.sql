-- Tags: no-replicated-database, no-shared-merge-tree

DROP TABLE IF EXISTS t_validate_huge_granularity;

-- Non-adaptive granularity stores wide parts only, so both wide-part thresholds must be 0.
-- ratio_of_defaults_for_sparse_serialization is pinned because the validator runs only on a
-- plain DEFAULT serialization: a sparse column skips it and the check becomes vacuous.
CREATE TABLE t_validate_huge_granularity (x UInt8)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS index_granularity_bytes = 0, index_granularity = 9223372036854775933, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_validate_huge_granularity SELECT number FROM numbers(100000);

SELECT count(), sum(x) FROM t_validate_huge_granularity;

DROP TABLE t_validate_huge_granularity;

-- A granule larger than one read batch must validate across batches, not stop after the first.
CREATE TABLE t_validate_multi_batch_granularity (x UInt8)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS index_granularity_bytes = 0, index_granularity = 2000000, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_validate_multi_batch_granularity SELECT number FROM numbers(4500000);

SELECT count(), sum(x) FROM t_validate_multi_batch_granularity;

DROP TABLE t_validate_multi_batch_granularity;

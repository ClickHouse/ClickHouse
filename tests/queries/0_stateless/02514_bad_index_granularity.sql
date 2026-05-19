CREATE TABLE t
(
    id Int64,
    d String,
    p Map(String, String)
)
ENGINE = ReplacingMergeTree order by id settings index_granularity = 0, index_granularity_bytes = 0; -- { serverError BAD_ARGUMENTS }

-- `index_granularity = 0` relies on byte-driven granularity; if `enable_mixed_granularity_parts`
-- is disabled there is no driver for granule sizing, so the combination must be rejected.
CREATE TABLE t2
(
    id Int64
)
ENGINE = MergeTree order by id settings index_granularity = 0, enable_mixed_granularity_parts = 0; -- { serverError BAD_ARGUMENTS }

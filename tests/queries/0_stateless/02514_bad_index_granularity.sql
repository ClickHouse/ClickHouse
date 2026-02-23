CREATE TABLE t
(
    id Int64,
    d String,
    p Map(String, String)
)
ENGINE = ReplacingMergeTree order by id settings index_granularity = 0, index_granularity_bytes = 0; -- { serverError BAD_ARGUMENTS }

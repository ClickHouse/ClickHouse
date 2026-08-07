CREATE TABLE t
(
    id Int64,
    d String,
    p Map(String, String)
)
ENGINE = ReplacingMergeTree order by id settings index_granularity = 0; -- { serverError BAD_ARGUMENTS }

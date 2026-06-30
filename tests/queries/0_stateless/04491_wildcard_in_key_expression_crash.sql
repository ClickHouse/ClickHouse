-- Regression test: `*` (and other column matchers) in PARTITION BY / ORDER BY
-- expanded to multiple columns by the Analyzer and crashed in
-- `registerStorageMergeTree` because `column_names` (one per AST child) was
-- shorter than `data_types` (one per resolved output column).

DROP TABLE IF EXISTS t_wildcard_partition_key;

CREATE TABLE t_wildcard_partition_key
(
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
PARTITION BY (*, b * b)
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_wildcard_partition_key
(
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY (*, b * b); -- { serverError BAD_ARGUMENTS }

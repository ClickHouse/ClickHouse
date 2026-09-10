-- Tags: no-old-analyzer
-- no-old-analyzer: Not supported

SET optimize_trivial_count_query = 1;

DROP TABLE IF EXISTS t_sparse_enum;

-- Enum8('a' = 1, 'b' = 2): type default is `'a'` (Field value 1), but
-- `IColumn::isDefaultAt` is bitwise so it only counts rows whose underlying
-- Int8 storage is 0, which is not a valid enum value here. Classifying
-- `enum_col = 'a'` as MatchesDefault against the persisted num_defaults
-- counter would return 0 for the trivial-count rewrite and drop matching
-- granules in the pruner. We reject these types and rely on the scan.
CREATE TABLE t_sparse_enum (e Enum8('a' = 1, 'b' = 2))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

INSERT INTO t_sparse_enum SELECT if(number < 4000, 'a', 'b')::Enum8('a' = 1, 'b' = 2) FROM numbers(5000);

-- Explicit CAST makes the analyzer keep the constant as Enum8 (not String), so
-- `tryGetLeastSupertype` returns Enum8 and the buggy classifier would mark
-- `e = CAST('a' ...)` as MatchesDefault even though `num_defaults` is 0.
SELECT 'rewrite_eq', count() FROM t_sparse_enum WHERE e = CAST('a' AS Enum8('a' = 1, 'b' = 2))
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'scan_eq',    count() FROM t_sparse_enum WHERE e = CAST('a' AS Enum8('a' = 1, 'b' = 2))
    SETTINGS optimize_trivial_count_with_sparsity_filter = 0;

SELECT 'rewrite_ne', count() FROM t_sparse_enum WHERE e != CAST('a' AS Enum8('a' = 1, 'b' = 2))
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'scan_ne',    count() FROM t_sparse_enum WHERE e != CAST('a' AS Enum8('a' = 1, 'b' = 2))
    SETTINGS optimize_trivial_count_with_sparsity_filter = 0;

DROP TABLE t_sparse_enum;

DROP TABLE IF EXISTS filter_constant_column_large_array;

CREATE TABLE filter_constant_column_large_array
(
    a Array(UInt64)
)
ENGINE = Memory;

INSERT INTO filter_constant_column_large_array SELECT range(1000001) FROM numbers(1);

SET query_plan_merge_filters = 0,
    query_plan_optimize_lazy_materialization = 0,
    query_plan_remove_unused_columns = 0;

SELECT 'enabled', length(a), dumpColumnStructure(a) LIKE '%Const%'
FROM filter_constant_column_large_array
WHERE a = range(1000001)
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT 'disabled', length(a), dumpColumnStructure(a) LIKE '%Const%'
FROM filter_constant_column_large_array
WHERE a = range(1000001)
SETTINGS optimize_constant_columns_after_filter = 0;

DROP TABLE filter_constant_column_large_array;

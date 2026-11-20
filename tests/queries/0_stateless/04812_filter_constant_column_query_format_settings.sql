DROP TABLE IF EXISTS 04812_filter_constant_column_query_format_settings;

CREATE TABLE 04812_filter_constant_column_query_format_settings
(
    b Bool
)
ENGINE = Memory;

INSERT INTO 04812_filter_constant_column_query_format_settings VALUES (false), (true);

SET query_plan_merge_filters = 0,
    query_plan_optimize_lazy_materialization = 0,
    query_plan_remove_unused_columns = 0;

SELECT 'enabled', toUInt8(b), dumpColumnStructure(b), count()
FROM 04812_filter_constant_column_query_format_settings
WHERE b = '0'
GROUP BY ALL
ORDER BY ALL
SETTINGS optimize_constant_columns_after_filter = 1,
    bool_true_representation = '0',
    bool_false_representation = '1';

SELECT 'disabled', toUInt8(b), dumpColumnStructure(b), count()
FROM 04812_filter_constant_column_query_format_settings
WHERE b = '0'
GROUP BY ALL
ORDER BY ALL
SETTINGS optimize_constant_columns_after_filter = 0,
    bool_true_representation = '0',
    bool_false_representation = '1';

DROP TABLE 04812_filter_constant_column_query_format_settings;

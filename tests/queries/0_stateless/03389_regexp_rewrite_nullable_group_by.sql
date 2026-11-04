-- https://github.com/ClickHouse/ClickHouse/issues/88218
-- RegexpFunctionRewritePass must handle Nullable result types from group_by_use_nulls
SELECT replaceRegexpOne(identity('abc123'), '^(abc)$', '\\1') GROUP BY 1, toLowCardinality(9), 1 WITH CUBE SETTINGS group_by_use_nulls=1;

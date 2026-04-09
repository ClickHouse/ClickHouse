-- Regression test: count_distinct_optimization must not apply when QUALIFY clause is present.
-- Previously this caused "Column identifier is already registered" exception.
SELECT DISTINCT countDistinct(y)
FROM (SELECT intDiv(number, 13) AS y, number + toNullable(11) AS x FROM system.numbers LIMIT 256)
QUALIFY countDistinct(x, toFixedString(materialize(toLowCardinality(toNullable('-- default'))), 10), y)
SETTINGS count_distinct_optimization = 1, enable_analyzer = 1;

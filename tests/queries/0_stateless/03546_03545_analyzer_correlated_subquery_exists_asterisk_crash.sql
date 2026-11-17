set enable_analyzer = 1;
set allow_experimental_correlated_subqueries = 1;

CREATE TABLE test (`i1` Int64, `i2` Int64, `i3` Int64, `i4` Int64, `i5` Int64, `i6` Int64, `i7` Int64, `i8` Int64, `i9` Int64, `i10` Int64) ENGINE = MergeTree ORDER BY tuple();

SELECT * * number
FROM numbers(
    exists(SELECT 1 FROM (SELECT        53, *,       materialize(toNullable(53)) FROM test AS t2 PREWHERE * OR (materialize(15) IS NOT NULL) WHERE * OR 1) LIMIT 1) = t1.i1, 5
    ) -- { serverError UNKNOWN_IDENTIFIER }

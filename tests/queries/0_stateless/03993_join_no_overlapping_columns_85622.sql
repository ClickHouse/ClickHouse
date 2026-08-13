-- https://github.com/ClickHouse/ClickHouse/issues/85622
-- https://github.com/ClickHouse/ClickHouse/issues/86528

CREATE TABLE test_85622 (key UInt64, a UInt8, b String, c Float64) ENGINE = MergeTree ORDER BY key;
INSERT INTO test_85622 SELECT number, number, toString(number), number FROM numbers(4);

SELECT (isZeroOrNull(toNullable(10)), *, *, *, *, materialize(toNullable(10)), *, toNullable(10) IS NULL) IS NOT NULL
FROM (SELECT '%avtomobili%', 2 + number AS key FROM numbers(4)) AS s
RIGHT JOIN test_85622 AS t USING (key)
ORDER BY s.key ASC NULLS LAST, t.key ASC NULLS FIRST;

DROP TABLE test_85622;

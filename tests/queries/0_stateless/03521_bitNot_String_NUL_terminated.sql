-- https://github.com/ClickHouse/ClickHouse/issues/80774
-- Regression for "Logical error: 'res.data[res.size] == '\0'" (in StringValueCompatibility)
SELECT uniqCombined64(bitNot(x)) IGNORE NULLS, anyHeavy(bitNot(x)) IGNORE NULLS FROM (SELECT DISTINCT concat(2, number) AS x FROM numbers(10)) FORMAT Null;
SELECT hex(any(bitNot('foo')));

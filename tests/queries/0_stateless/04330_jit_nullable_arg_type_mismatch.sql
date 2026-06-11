-- Tags: no-fasttest
-- no-fasttest: requires the embedded compiler (JIT).

SET group_by_use_nulls = 1;
SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

-- A compiled function whose resolved argument type is non-nullable but whose actual
-- child column is Nullable (group_by_use_nulls makes grouping keys nullable) must not
-- abort JIT codegen. The query result must match the interpreter (NULL row).
SELECT (SELECT bitNot(bitCount(number)) / toInt128(-2147483649)
        QUALIFY equals(number, bitCount(bitCount(plus(-inf, number))) * materialize(2147483647)))
FROM numbers_mt(3)
GROUP BY GROUPING SETS ((), (), (*))
QUALIFY 255;

SELECT bitCount(plus(1.0, number)) AS x
FROM numbers(4)
GROUP BY GROUPING SETS ((), (number))
ORDER BY x;

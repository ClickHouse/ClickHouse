-- Reproducer for logical error in caseWithExpression with Nullable(Nothing) arguments.
-- The issue was that equals() returning ColumnConst(ColumnNullable(ColumnNothing)) was not detected
-- as nullable by isColumnNullable(), causing a type mismatch in the if() function.

SET enable_analyzer = 1;

SELECT DISTINCT
    caseWithExpression(1, *, 1, materialize(NULL)),
    assumeNotNull(1),
    and(caseWithExpression(materialize(toNullable(1)), materialize(NULL), assumeNotNull(isNull(1)), 1), 1, *)
FROM numbers(10)
WHERE isNotNull(1)
QUALIFY assumeNotNull(assumeNotNull(isNotNull(1)))
ORDER BY ALL DESC
OFFSET 1048577;

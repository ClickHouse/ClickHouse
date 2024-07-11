-- Test SQL function 'generateSnowflakeID'

SELECT bitAnd(bitShiftRight(toUInt64(generateSnowflakeID()), 63), 1) = 0; -- check first bit is zero
SELECT generateSnowflakeID() = generateSnowflakeID(1); -- same as ^^
SELECT generateSnowflakeID(1) = generateSnowflakeID(1); -- enabled common subexpression elimination

SELECT generateSnowflakeID(1) = generateSnowflakeID(2); -- disabled common subexpression elimination --> lhs != rhs
SELECT generateSnowflakeID(1) != generateSnowflakeID(); -- Check different invocations yield different results

SELECT generateSnowflakeID('expr', 1) = generateSnowflakeID('expr', 1); -- enabled common subexpression elimination
SELECT generateSnowflakeID('expr', 1) != generateSnowflakeID('expr', 2); -- different machine IDs should produce different results
SELECT generateSnowflakeID('expr', 1) != generateSnowflakeID('different_expr', 1); -- different expressions should bypass common subexpression elimination

SELECT count(*)
FROM
(
    SELECT DISTINCT generateSnowflakeID()
    FROM numbers(100)
);

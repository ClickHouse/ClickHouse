SELECT '-- generateSnowflakeID';

SELECT bitAnd(bitShiftRight(toUInt64(generateSnowflakeID()), 63), 1) = 0; -- check first bit is zero

SELECT generateSnowflakeID(1) = generateSnowflakeID(2); -- disabled common subexpression elimination --> lhs != rhs
SELECT generateSnowflakeID() = generateSnowflakeID(1); -- same as ^^
SELECT generateSnowflakeID(1) = generateSnowflakeID(1); -- enabled common subexpression elimination

SELECT generateSnowflakeID(1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT count(*)
FROM
(
    SELECT DISTINCT generateSnowflakeID()
    FROM numbers(100)
);

SELECT '-- generateSnowflakeIDThreadMonotonic';

SELECT bitAnd(bitShiftRight(toUInt64(generateSnowflakeIDThreadMonotonic()), 63), 1) = 0; -- check first bit is zero

SELECT generateSnowflakeIDThreadMonotonic(1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT count(*)
FROM
(
    SELECT DISTINCT generateSnowflakeIDThreadMonotonic()
    FROM numbers(100)
);

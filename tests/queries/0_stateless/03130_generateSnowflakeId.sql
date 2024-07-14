-- Test SQL function 'generateSnowflakeID'

SELECT bitAnd(bitShiftRight(toUInt64(generateSnowflakeID()), 63), 1) = 0; -- check first bit is zero
SELECT generateSnowflakeID() = generateSnowflakeID(1); -- same as ^^
SELECT generateSnowflakeID(1) = generateSnowflakeID(2); -- disabled common subexpression elimination --> lhs != rhs
SELECT generateSnowflakeID(1) = generateSnowflakeID(1); -- enabled common subexpression elimination

SELECT generateSnowflakeID('expr', 1) = generateSnowflakeID('expr', 1); -- enabled common subexpression elimination
SELECT generateSnowflakeID('expr', 1) != generateSnowflakeID('expr', 2); -- different machine IDs should produce different results

SELECT bitAnd(generateSnowflakeID(1023), 1023) = 1023; -- check if the last 10 bits match the machine ID

SELECT generateSnowflakeID('invalid_machine_id'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT generateSnowflakeID(materialize(toUInt64(1))) = generateSnowflakeID(materialize(toUInt64(1))); -- Test with non-const machine ID

SELECT count(*)
FROM
(
    SELECT DISTINCT generateSnowflakeID()
    FROM numbers(100)
);

-- Test SQL function 'generateSnowflakeID'

SELECT 'Negative tests';
SELECT generateSnowflakeID(1, 2, 3); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT generateSnowflakeID(1, 'not_an_int'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT generateSnowflakeID(1, materialize(2)); -- { serverError ILLEGAL_COLUMN }

 SELECT 'The first bit must be zero';
SELECT bitAnd(bitShiftRight(generateSnowflakeID(), 63), 1) = 0;

SELECT 'Test disabling of common subexpression elimination via first parameter';
SELECT generateSnowflakeID(1) = generateSnowflakeID(2); -- disabled common subexpression elimination --> lhs != rhs
SELECT generateSnowflakeID() = generateSnowflakeID(1); -- same as ^^
SELECT generateSnowflakeID(1) = generateSnowflakeID(1); -- with common subexpression elimination

SELECT 'Test user-provided machine ID';
SELECT bitAnd(bitShiftRight(generateSnowflakeID(1, 123), 12), 1024 - 1) = 123; -- the machine id is actually set in the generated snowflake ID (1024 = 2^10)

SELECT 'Generated Snowflake IDs are unique';
SELECT count(*)
FROM
(
    SELECT DISTINCT generateSnowflakeID()
    FROM numbers(100)
);

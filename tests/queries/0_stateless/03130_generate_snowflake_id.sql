SELECT bitShiftLeft(toUInt64(generateSnowflakeID()), 52) = 0;
SELECT bitAnd(bitShiftRight(toUInt64(generateSnowflakeID()), 63), 1) = 0;

SELECT generateSnowflakeID(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT count(*)
FROM
(
    SELECT DISTINCT generateSnowflakeID()
    FROM numbers(10)
)
SELECT toTypeName(xxHash64Spark('ABC'));
SELECT xxHash64Spark('ABC') = toInt64(4105715581806190027);
SELECT xxHash64Spark(NULL) = toInt64(42);
SELECT xxHash64Spark(if(number = 0, NULL, 'ABC')) = if(number = 0, toInt64(42), toInt64(4105715581806190027)) FROM numbers(2) ORDER BY number;

SELECT xxHash64Spark(toUInt8(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT xxHash64Spark(toUInt16(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT xxHash64Spark(toUInt32(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT xxHash64Spark('a', 'b'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

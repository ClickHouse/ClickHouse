SELECT toTypeName(xxHash64Spark('ABC'));
SELECT xxHash64Spark('ABC') = toInt64(4105715581806190027);
SELECT xxHash64Spark(NULL) = toInt64(42);
SELECT xxHash64Spark(if(number = 0, NULL, 'ABC')) = if(number = 0, toInt64(42), toInt64(4105715581806190027)) FROM numbers(2) ORDER BY number;

-- Negative tests

-- Only strings can be hashed at the moment
SELECT xxHash64Spark(toUInt8(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT xxHash64Spark(toUInt16(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT xxHash64Spark(toUInt32(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT xxHash64Spark(toUInt64(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT xxHash64Spark(toUInt128(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT xxHash64Spark(toUInt256(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT xxHash64Spark(toFloat32(0.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Only single arguments are allowed at the moment
SELECT xxHash64Spark('a', 'b'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

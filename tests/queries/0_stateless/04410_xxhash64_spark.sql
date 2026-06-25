SELECT toTypeName(xxHash64Spark('ABC'));
SELECT xxHash64Spark('ABC') = toInt64(4105715581806190027);
SELECT xxHash64Spark(NULL) IS NULL;

SELECT xxHash64Spark(toUInt8(0)) = xxHash64Spark('\0');
SELECT xxHash64Spark(toUInt16(0)) = xxHash64Spark('\0\0');
SELECT xxHash64Spark(toUInt32(0)) = xxHash64Spark('\0\0\0\0');

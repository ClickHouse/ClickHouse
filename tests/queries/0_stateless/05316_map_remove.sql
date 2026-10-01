SELECT
    mapRemove(map('a', 1, 'b', 2), 'a'),
    mapRemove(map('a', 1, 'b', 2), 'missing'),
    mapRemove(map('a', 1), 'a'),
    mapRemove(mapFromArrays(emptyArrayString(), emptyArrayUInt8()), 'a')
FORMAT TabSeparatedRaw;

SELECT mapRemove(map('a', 1, 'a', 2, 'b', 3), 'a') FORMAT TabSeparatedRaw;

SELECT mapRemove(map(toUInt64(1), 'one', toUInt64(2), 'two'), toUInt8(1)) FORMAT TabSeparatedRaw;

SELECT mapRemove(map('a', 1, 'b', 2), CAST(NULL, 'Nullable(String)')) FORMAT TabSeparatedRaw;

SELECT number, mapRemove(map('a', 1, 'b', 2), if(number = 0, toNullable('a'), CAST(NULL, 'Nullable(String)'))) FROM numbers(2) ORDER BY number FORMAT TabSeparatedRaw;

SELECT mapRemove(
    map(tuple(CAST(NULL, 'Nullable(UInt8)'), toUInt8(1)), 'null', tuple(toNullable(toUInt8(5)), toUInt8(1)), 'five'),
    tuple(CAST(NULL, 'Nullable(UInt8)'), toUInt8(1))) FORMAT TabSeparatedRaw;

SELECT mapRemove(map('a', CAST(NULL, 'Nullable(UInt8)'), 'b', toNullable(toUInt8(2))), 'b') FORMAT TabSeparatedRaw;

SELECT number, mapRemove(map('a', number, 'b', number + 1), 'a') FROM numbers(2) ORDER BY number FORMAT TabSeparatedRaw;

SELECT number, mapRemove(map('a', 1, 'b', 2), if(number = 0, 'a', 'b')) FROM numbers(2) ORDER BY number FORMAT TabSeparatedRaw;

SELECT number, mapRemove(map('a', number, 'b', number + 1), if(number = 0, 'a', 'b')) FROM numbers(2) ORDER BY number FORMAT TabSeparatedRaw;

SELECT toTypeName(mapRemove(CAST(map('a', 'x', 'b', 'y'), 'Map(LowCardinality(String), LowCardinality(String))'), 'a')) FORMAT TabSeparatedRaw;

SELECT mapRemove(CAST(map('a', 'x', 'b', 'y'), 'Map(LowCardinality(String), LowCardinality(String))'), 'a') FORMAT TabSeparatedRaw;

SELECT mapRemove([1, 2], 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapRemove(map('a', 1)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapRemove(map('a', 1), [1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT, NO_COMMON_TYPE }

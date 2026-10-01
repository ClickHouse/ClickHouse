SELECT mapEntries(map('a', 1, 'b', 2)) FORMAT TabSeparatedRaw;

SELECT mapEntries(map('a', 1, 'a', 2, 'b', 3)) FORMAT TabSeparatedRaw;

SELECT mapEntries(mapFromArrays(emptyArrayString(), emptyArrayUInt8())) FORMAT TabSeparatedRaw;

SELECT toTypeName(mapEntries(CAST(map('a', 1), 'Map(String, UInt64)'))) FORMAT TabSeparatedRaw;

SELECT number, mapEntries(map('a', number, 'b', number + 1)) FROM numbers(2) ORDER BY number FORMAT TabSeparatedRaw;

SELECT mapEntries(map('a', CAST(NULL, 'Nullable(UInt8)'), 'b', toNullable(toUInt8(2)))) FORMAT TabSeparatedRaw;

SELECT mapEntries(CAST(map('a', 'x', 'b', 'y'), 'Map(LowCardinality(String), LowCardinality(String))')) FORMAT TabSeparatedRaw;

SELECT toTypeName(mapEntries(CAST(map('a', 'x'), 'Map(LowCardinality(String), LowCardinality(String))'))) FORMAT TabSeparatedRaw;

SELECT mapEntries(CAST(map('a', [1, 2], 'b', [3]), 'Map(String, Array(UInt64))')) FORMAT TabSeparatedRaw;

SELECT mapEntries([1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapEntries(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapEntries(map('a', 1), map('b', 2)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

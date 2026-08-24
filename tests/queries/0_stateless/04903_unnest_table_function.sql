SELECT * FROM unnest([1, 2, 3]);
SELECT * FROM UNNEST(['a', 'b']);
SELECT count() FROM unnest([]);
SELECT count() FROM unnest(CAST([] AS Array(UInt8)));
SELECT * FROM unnest([[1, 2], [3]]);
SELECT count() FROM unnest(CAST(NULL AS Nullable(Array(UInt8))));
DESC unnest([1, 2, 3]);
DESC unnest(CAST([] AS Array(UInt8)));
DESC unnest(CAST(NULL AS Nullable(Array(UInt8))));
SELECT * FROM unnest(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * FROM unnest(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM unnest([1], [2]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

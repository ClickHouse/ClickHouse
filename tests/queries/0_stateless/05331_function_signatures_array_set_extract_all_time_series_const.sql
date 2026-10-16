-- `arrayUnion` keeps rejecting a mix of `Decimal` and non-`Decimal` elements:
-- its declarative signature is documentation-only.
SELECT arrayUnion([toDecimal32(1, 1)], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arraySort(arrayUnion([1, 2], [2, 3])) AS u, toTypeName(u);
SELECT arrayIntersect([1, 2], [2, 3]) AS i, toTypeName(i);
SELECT arraySort(arraySymmetricDifference([1, 2], [2, 3])) AS s, toTypeName(s);

-- The regexp of `extractAll` and the tag arguments of the `timeSeries*Tag*` functions must be constant,
-- and their signatures say so.
SELECT extractAll('abcb', materialize('b')); -- { serverError ILLEGAL_COLUMN }
SELECT name, signature FROM system.functions
WHERE name IN ('extractAll', 'timeSeriesJoinTags', 'timeSeriesRemoveTag', 'timeSeriesRemoveTags', 'timeSeriesCopyTag',
    'timeSeriesCopyTags', 'timeSeriesExtractTag', 'timeSeriesRemoveAllTagsExcept', 'timeSeriesReplaceTag')
ORDER BY name;

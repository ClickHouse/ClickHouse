-- The setting `allow_nullable_tuple_in_extracted_subcolumns` can only be changed via server restart

SET enable_analyzer = 1;
SET allow_nullable_tuple_in_extracted_subcolumns = 1;

SELECT
    toTypeName(v.`Tuple(UInt64, String)`),
    v.`Tuple(UInt64, String)`
FROM
(
    SELECT 42::Variant(Tuple(UInt64, String), UInt64) AS v
);

SET allow_nullable_tuple_in_extracted_subcolumns = 0;

SELECT
    toTypeName(v.`Tuple(UInt64, String)`),
    v.`Tuple(UInt64, String)`
FROM
(
    SELECT 42::Variant(Tuple(UInt64, String), UInt64) AS v
);

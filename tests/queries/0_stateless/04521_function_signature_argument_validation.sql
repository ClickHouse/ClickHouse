-- Argument validation restored / tightened by the declarative function signatures.
-- https://github.com/ClickHouse/ClickHouse/pull/104948

-- pointInPolygon: the point tuple must hold native numbers. A non-numeric point is rejected with a
-- clean ILLEGAL_TYPE_OF_ARGUMENT during analysis instead of reaching the raw CallPointInPolygon
-- dispatch and raising a LOGICAL_ERROR ("Unknown numeric column type") at execution.
SELECT pointInPolygon((0.5, 0.5), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]);
SELECT pointInPolygon((3, 3), [(6, 0), (8, 4), (5, 8), (0, 2)]);
SELECT pointInPolygon(('a', 'b'), [(0., 0.), (1., 0.), (1., 1.)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT pointInPolygon((toDecimal64(1, 2), toDecimal64(1, 2)), [(0., 0.), (1., 0.), (1., 1.)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- sparseGramsHashes: only a String input and constant integer options are accepted (the body reads
-- the options once per block, so row-varying option columns are not allowed).
SELECT length(sparseGramsHashes('alice', 3));
SELECT sparseGramsHashes(123, 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sparseGramsHashes(materialize('alice'), materialize(toUInt32(3))); -- { serverError ILLEGAL_COLUMN }

-- YYYYMMDDhhmmssToDateTime64: a Decimal-typed precision is interpreted by its logical value, so the
-- declared result type and the executed column agree on the scale (0.001 -> scale 0).
SELECT toTypeName(YYYYMMDDhhmmssToDateTime64(20230911131415, 0.001 :: Decimal(18, 3), 'UTC'));
SELECT YYYYMMDDhhmmssToDateTime64(20230911131415, 0.001 :: Decimal(18, 3), 'UTC');

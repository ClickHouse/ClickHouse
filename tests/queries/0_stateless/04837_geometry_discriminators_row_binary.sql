-- The `Geometry` type is a `Variant` whose global discriminators are persisted in data, so they
-- must never change: new geo types are only appended at the end with the next free discriminator.
-- Decode one value per discriminator from `RowBinary` (a discriminator byte followed by the value)
-- to pin the complete table. See also 04511_geometry_discriminators_compatibility, which reads
-- blobs produced by a pre-`MultiPoint` server; this test additionally pins `MultiPoint` = 6.

SELECT g, variantType(g) FROM format(RowBinary, 'g Geometry', unhex('0002000000000000F03F000000000000004000000000000008400000000000001040')); -- 0 = LineString
SELECT g, variantType(g) FROM format(RowBinary, 'g Geometry', unhex('010102000000000000F03F000000000000004000000000000008400000000000001040')); -- 1 = MultiLineString
SELECT g, variantType(g) FROM format(RowBinary, 'g Geometry', unhex('02010102000000000000F03F000000000000004000000000000008400000000000001040')); -- 2 = MultiPolygon
SELECT g, variantType(g) FROM format(RowBinary, 'g Geometry', unhex('03000000000000F03F0000000000000040')); -- 3 = Point
SELECT g, variantType(g) FROM format(RowBinary, 'g Geometry', unhex('040102000000000000F03F000000000000004000000000000008400000000000001040')); -- 4 = Polygon
SELECT g, variantType(g) FROM format(RowBinary, 'g Geometry', unhex('0502000000000000F03F000000000000004000000000000008400000000000001040')); -- 5 = Ring
SELECT g, variantType(g) FROM format(RowBinary, 'g Geometry', unhex('0602000000000000F03F000000000000004000000000000008400000000000001040')); -- 6 = MultiPoint

-- NULL is discriminator 255.
SELECT g, variantType(g) FROM format(RowBinary, 'g Geometry', unhex('FF'));

-- 7 is the next free discriminator: nothing must decode from it until an eighth geo type is added.
SELECT g FROM format(RowBinary, 'g Geometry', unhex('07000000000000F03F0000000000000040')); -- { serverError INCORRECT_DATA }

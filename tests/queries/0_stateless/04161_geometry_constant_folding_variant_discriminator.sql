-- Constant folding must preserve the Variant discriminator of a named Variant (Geometry).
-- Geometry has layout-duplicate alternatives (LineString/Ring are Array(Point),
-- MultiLineString/Polygon are Array(Array(Point)), empties are layout-compatible with all),
-- so two constants that differ only in their alternative render to the same value string.
-- They must not collapse to one action node in the DAG. See issue #110698.

SET enable_analyzer = 1;

-- Empty geometries: three distinct types that all fold to an empty array.
SELECT wkt(readWKT('LINESTRING EMPTY')), wkt(readWKT('MULTILINESTRING EMPTY')), wkt(readWKT('MULTIPOLYGON EMPTY'));
SELECT hex(wkb(readWKT('LINESTRING EMPTY'))), hex(wkb(readWKT('MULTILINESTRING EMPTY'))), hex(wkb(readWKT('MULTIPOLYGON EMPTY')));

-- Non-empty layout-duplicate alternatives with identical points.
SELECT wkt(readWKT('LINESTRING(0 0,1 0,0 0)')), wkt(readWKT('MULTILINESTRING((0 0,1 0,0 0))')), wkt(readWKT('POLYGON((0 0,1 0,0 0))'));

-- Results must match the pre-folding path (row columns) exactly.
SELECT wkt(readWKT(l)), wkt(readWKT(m)), wkt(readWKT(p))
FROM (SELECT materialize('LINESTRING EMPTY') AS l, materialize('MULTILINESTRING EMPTY') AS m, materialize('MULTIPOLYGON EMPTY') AS p);

-- Same root cause via Dynamic: the runtime type is part of the value, and two constants that
-- differ only in their dynamic type must not collapse. The type name (not the per-column
-- numeric discriminator) has to be part of the action node name.
SELECT number, dynamicType(if(number = 0, CAST(toUInt8(1), 'Dynamic'), CAST(toUInt16(1), 'Dynamic'))) FROM numbers(2);
SELECT number, dynamicType(multiIf(number = 0, CAST(CAST([1], 'Array(UInt8)'), 'Dynamic'), CAST(CAST([1], 'Array(UInt16)'), 'Dynamic'))) FROM numbers(2);

-- Dynamic(max_types=0): single-type constants are stored in the shared variant (not a typed
-- variant), so this exercises the discr == getSharedVariantDiscriminator() branch of
-- ColumnDynamic::getValueNameImpl. Without the type-name prefix both constants render to the
-- same string and collapse to one action node.
SELECT number, dynamicType(if(number = 0, CAST(toUInt8(1), 'Dynamic(max_types=0)'), CAST(toUInt16(1), 'Dynamic(max_types=0)'))) FROM numbers(2);
SELECT number, dynamicType(multiIf(number = 0, CAST(CAST([1], 'Array(UInt8)'), 'Dynamic(max_types=0)'), CAST(CAST([1], 'Array(UInt16)'), 'Dynamic(max_types=0)'))) FROM numbers(2);

-- Same root cause via an explicit Variant with layout-duplicate alternatives.
SET allow_suspicious_variant_types = 1;
SELECT number, variantType(if(number = 0, CAST(toUInt8(1), 'Variant(UInt8, UInt16)'), CAST(toUInt16(1), 'Variant(UInt8, UInt16)'))) FROM numbers(2);

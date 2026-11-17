-- The constant-polygon preprocessed cache is keyed by the raw constant arguments (not by the
-- parsed geometry), so repeated invocations skip re-parsing the polygon. The key must fold in
-- the validate_polygons setting and the argument types, otherwise the same raw bytes under a
-- different mode/type would alias to a wrong cache entry. Parsing (and validation) runs only on
-- a cache miss, so anything that changes how the raw bytes are interpreted belongs in the key.

-- validate_polygons in the key: the same invalid polygon must be rejected with validation on and
-- accepted (validation bypassed) with validation off, in BOTH orders. If validate_polygons were
-- not in the key, the first lookup would populate the cache and the second would reuse it,
-- skipping or wrongly applying validation.
SELECT pointInPolygon((0.5, 0.5), [(0., 0.), (1., 1.), (1., 0.), (0., 1.)]) SETTINGS validate_polygons = 1; -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((0.5, 0.5), [(0., 0.), (1., 1.), (1., 0.), (0., 1.)]) SETTINGS validate_polygons = 0;
-- Reverse order with a different invalid polygon (a validate = 0 entry must not satisfy validate = 1).
SELECT pointInPolygon((2.5, 2.5), [(0., 0.), (3., 3.), (3., 0.), (0., 3.)]) SETTINGS validate_polygons = 0;
SELECT pointInPolygon((2.5, 2.5), [(0., 0.), (3., 3.), (3., 0.), (0., 3.)]) SETTINGS validate_polygons = 1; -- { serverError BAD_ARGUMENTS }

-- A failed validation must not poison the cache: a later valid query for a different polygon and a
-- later validate = 0 query for the rejected polygon must both succeed (the entry is inserted only
-- on a successful parse).
SELECT pointInPolygon((0.5, 0.5), [(0., 0.), (1., 1.), (1., 0.), (0., 1.)]) SETTINGS validate_polygons = 1; -- { serverError BAD_ARGUMENTS }
SELECT pointInPolygon((0.5, 0.5), [(0., 0.), (0., 1.), (1., 1.), (1., 0.)]) SETTINGS validate_polygons = 1;
SELECT pointInPolygon((0.5, 0.5), [(0., 0.), (1., 1.), (1., 0.), (0., 1.)]) SETTINGS validate_polygons = 0;

-- Argument types in the key: coordinates are cast to Float64 during parsing, so polygons that
-- differ only by argument type must be cached independently. This needs a value whose byte is
-- shared by Int8 and UInt8 but casts to a different Float64; a negative Int8 (high bit set) does
-- that, so we use -1 (byte 0xFF -> Int8 -1.0 vs UInt8 255.0). A non-negative value like 0 or 1
-- encodes identically in both types and casts to the same Float64, so it could not expose the
-- aliasing (the test would pass even with the type omitted from the key). The raw value bytes are
-- identical across the two types (Int8 -1 and UInt8 255 are both byte 0xFF; Int8 3 and UInt8 3 are
-- both 0x03), so without the type in the key the second polygon would alias to the first cache
-- entry and return its result. We check both insertion orders without touching the shared cache by
-- using two byte-disjoint pairs (so the pairs never collide and no eviction is needed):
--   Pair A (bytes 0xFF/0x03) inserts the Int8 polygon first, then the UInt8 one.
--   Pair B (bytes 0xFE/0x04) inserts the UInt8 polygon first, then the Int8 one.
-- For each pair the Int8 box contains (0, 0) (-> 1) and the UInt8 box does not (-> 0); aliasing
-- would make the second query of a pair echo the first's result.
SELECT pointInPolygon((0., 0.), [(-1, -1), (-1, 3), (3, 3), (3, -1)]::Array(Tuple(Int8, Int8)));
SELECT pointInPolygon((0., 0.), [(255, 255), (255, 3), (3, 3), (3, 255)]::Array(Tuple(UInt8, UInt8)));
SELECT pointInPolygon((0., 0.), [(254, 254), (254, 4), (4, 4), (4, 254)]::Array(Tuple(UInt8, UInt8)));
SELECT pointInPolygon((0., 0.), [(-2, -2), (-2, 4), (4, 4), (4, -2)]::Array(Tuple(Int8, Int8)));

-- Multipolygon on the same (shared) cache: keyed independently from the single polygons above.
SELECT pointInPolygon((2., 2.), [[[(0., 0.), (3., 0.), (3., 3.), (0., 3.)]], [[(4., 4.), (8., 4.), (8., 8.), (4., 8.)]]]);
SELECT pointInPolygon((6., 6.), [[[(0., 0.), (3., 0.), (3., 3.), (0., 3.)]], [[(4., 4.), (8., 4.), (8., 8.), (4., 8.)]]]);

-- The optimization still works: repeated invocations over many blocks parse the constant polygon
-- once and return the same result. Functional correctness of the cached path under many blocks.
SELECT sum(pointInPolygon((number % 10, intDiv(number, 10) % 10), [(0.5, 0.5), (8.5, 0.5), (8.5, 8.5), (0.5, 8.5)]))
FROM numbers_mt(1000000)
SETTINGS max_threads = 8, max_block_size = 4096;

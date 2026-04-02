-- Regression test for WKB parsing allocation overflow (STID 1290-1eae).
-- Crafted WKB binary with huge element counts must throw BAD_ARGUMENTS
-- instead of attempting multi-GB memory allocations (which crash sanitizer builds).
-- The max_wkb_points setting controls the limit.

-- LineString: num_points = 0xFFFFFFFF (4 billion)
-- WKB: 01 (LE) + 02000000 (LineString) + FFFFFFFF (num_points)
SELECT readWKBLineString(unhex('0102000000FFFFFFFF')); -- {serverError BAD_ARGUMENTS}

-- Polygon: num_rings = 0xFFFFFFFF
-- WKB: 01 (LE) + 03000000 (Polygon) + FFFFFFFF (num_rings)
SELECT readWKBPolygon(unhex('0103000000FFFFFFFF')); -- {serverError BAD_ARGUMENTS}

-- Polygon with 1 ring but ring has num_points = 0xF0000005 (~4 billion)
-- WKB: 01 (LE) + 03000000 (Polygon) + 01000000 (1 ring) + 050000F0 (huge point count, LE)
SELECT readWKBPolygon(unhex('010300000001000000050000F0')); -- {serverError BAD_ARGUMENTS}

-- MultiLineString: num_lines = 0xFFFFFFFF
-- WKB: 01 (LE) + 05000000 (MultiLineString) + FFFFFFFF (num_lines)
SELECT readWKBMultiLineString(unhex('0105000000FFFFFFFF')); -- {serverError BAD_ARGUMENTS}

-- MultiPolygon: num_polygons = 0xFFFFFFFF
-- WKB: 01 (LE) + 06000000 (MultiPolygon) + FFFFFFFF (num_polygons)
SELECT readWKBMultiPolygon(unhex('0106000000FFFFFFFF')); -- {serverError BAD_ARGUMENTS}

-- Generic readWKB: Polygon with 1 ring, ring has num_points = 0x7FFFFFFF
-- WKB: 01 (LE) + 03000000 (Polygon) + 01000000 (1 ring) + FFFFFF7F (2 billion points, LE)
SELECT readWKB(unhex('010300000001000000FFFFFF7F')); -- {serverError BAD_ARGUMENTS}

-- Verify valid data still works
SELECT readWKBPoint(unhex('0101000000000000000000f03f0000000000000040'));
SELECT readWKBLineString(unhex('010200000002000000000000000000f03f000000000000f03f00000000000000400000000000000040'));
SELECT readWKBPolygon(unhex('010300000001000000040000000000000000000000000000000000000000000000000024400000000000000000000000000000244000000000000024400000000000000000000000000000000000000000000000000000000000000000'));

-- Verify the setting is configurable: a low limit rejects even small valid geometry
SELECT readWKBLineString(unhex('010200000004000000000000000000f03f000000000000f03f0000000000000040000000000000004000000000000008400000000000000840000000000000f03f000000000000f03f')) SETTINGS max_wkb_points = 2; -- {serverError BAD_ARGUMENTS}

-- Test WKB parsing bounds checking and max_wkb_geometry_elements setting.

-- WKB LineString with num_points = 0xFFFFFFFF (4 billion) — exceeds hard limit.
SELECT readWKBLineString(unhex('0102000000FFFFFFFF')); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- WKB Polygon with num_rings = 0x10000000 (268 million) — exceeds hard limit.
SELECT readWKBPolygon(unhex('010300000000000010')); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- WKB MultiPolygon with num_polygons = 0x01000000 (16 million) — exceeds default 1M limit.
SELECT readWKBMultiPolygon(unhex('010600000000000001')); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- Test max_wkb_geometry_elements setting: lower the limit to 2 elements.
SET max_wkb_geometry_elements = 2;

-- A LineString with 3 points should now fail.
SELECT readWKBLineString(unhex('01020000000300000000000000000000000000000000000000000000000000F03F000000000000F03F00000000000000400000000000000040')); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- A LineString with 2 points should succeed.
SELECT readWKBLineString(unhex('01020000000200000000000000000000000000000000000000000000000000F03F000000000000F03F'));

-- Reset and test readWKB (variant-returning) with bad data
SET max_wkb_geometry_elements = 1000000;
SELECT readWKB(unhex('0102000000FFFFFFFF')); -- { serverError TOO_LARGE_ARRAY_SIZE }

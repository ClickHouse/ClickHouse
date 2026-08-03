-- Test: a zero-area box (one grid axis has zero items) must return the single
-- covering geohash via the `items == 0` fallback, not an empty array.

-- Equal latitudes: longitude axis wide, latitude axis has zero items.
SELECT geohashesInBox(0., 0., 180., 0., 12);
-- Negative-zero latitude max (-0.0 is not < 0.0) is still a zero-area box.
SELECT geohashesInBox(0., 0., 180., -0., 12);
-- Equal longitudes: latitude axis wide, longitude axis has zero items.
SELECT geohashesInBox(0., 0., 0., 90., 12);
-- Float32 coordinates take the other execute<> instantiation, same fallback.
SELECT geohashesInBox(toFloat32(0.), toFloat32(0.), toFloat32(180.), toFloat32(0.), 12);
-- Length is exactly one on the fallback path.
SELECT length(geohashesInBox(0., 0., 180., 0., 12));
-- Non-constant (per-row) dispatch reaches the same fallback for every row.
SELECT DISTINCT geohashesInBox(materialize(0.), materialize(0.), materialize(180.), materialize(0.), materialize(toUInt8(12))) FROM numbers(3);

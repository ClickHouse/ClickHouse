-- Test: a zero-area (point) box has zero items on both grid axes, so it must
-- return the single geohash covering that point via the `items == 0` fallback,
-- not an empty array. A point box is used deliberately: the covering geohash is
-- well-defined there, unlike a line-degenerate box whose value semantics are
-- unresolved (see 04694_geohashes_in_box_degenerate_span.sh, termination only).

-- Point box at the origin: both grid axes have zero items.
SELECT geohashesInBox(0., 0., 0., 0., 12);
-- Negative-zero coordinate (-0.0 is not < 0.0) is still the same point box.
SELECT geohashesInBox(0., 0., 0., -0., 12);
-- Off-origin point box: the fallback encodes the actual corner, not a constant.
SELECT geohashesInBox(180., 90., 180., 90., 12);
-- Float32 coordinates take the other execute<> instantiation, same fallback.
SELECT geohashesInBox(toFloat32(0.), toFloat32(0.), toFloat32(0.), toFloat32(0.), 12);
-- Length is exactly one on the fallback path.
SELECT length(geohashesInBox(0., 0., 0., 0., 12));
-- Non-constant (per-row) dispatch reaches the same fallback for every row.
SELECT DISTINCT geohashesInBox(materialize(0.), materialize(0.), materialize(0.), materialize(0.), materialize(toUInt8(12))) FROM numbers(3);

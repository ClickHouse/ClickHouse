-- Empty polygon must round-trip through WKB/WKT canonically: numRings = 0, not a spurious
-- single zero-point ring. See https://github.com/ClickHouse/ClickHouse/issues/110701

-- WKT source: canonical WKB of POLYGON EMPTY is type 3 with numRings = 0.
SELECT hex(wkb(readWKTPolygon('POLYGON EMPTY')));
-- Column representation of an empty polygon is an empty array of rings.
SELECT readWKTPolygon('POLYGON EMPTY');

-- WKB source (canonical numRings = 0) round-trips to itself (identity-preserving).
SELECT hex(wkb(readWKBPolygon(unhex('010300000000000000'))));
-- WKB source that already carries the non-canonical single empty ring is normalized to numRings = 0.
SELECT hex(wkb(readWKBPolygon(unhex('01030000000100000000000000'))));

-- Non-empty polygon is unaffected.
SELECT hex(wkb(readWKTPolygon('POLYGON((1 0, 10 0, 10 10, 0 10, 1 0))')));

-- Empty polygon inside a table column, both WKB round-trip and stored value.
DROP TABLE IF EXISTS t_empty_polygon;
CREATE TABLE t_empty_polygon (a Polygon) ENGINE = Memory();
INSERT INTO t_empty_polygon VALUES ([]);
SELECT hex(wkb(a)), a FROM t_empty_polygon;
DROP TABLE t_empty_polygon;

-- A column already holding the non-canonical [[]] shape (one empty ring, e.g. rows
-- written by an older build) must still serialize to canonical WKB via wkb().
DROP TABLE IF EXISTS t_noncanon_polygon;
CREATE TABLE t_noncanon_polygon (a Polygon) ENGINE = Memory();
INSERT INTO t_noncanon_polygon VALUES ([[]]);
SELECT hex(wkb(a)) FROM t_noncanon_polygon;
DROP TABLE t_noncanon_polygon;

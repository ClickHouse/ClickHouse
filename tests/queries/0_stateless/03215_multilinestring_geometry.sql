-- { echoOn }
SELECT readWKTMultiLineString('MULTILINESTRING ((1 1, 2 2, 3 3, 1 1))');
SELECT toTypeName(readWKTMultiLineString('MULTILINESTRING ((1 1, 2 2, 3 3, 1 1))'));
SELECT wkt(readWKTMultiLineString('MULTILINESTRING ((1 1, 2 2, 3 3, 1 1))'));

SELECT readWKTMultiLineString('MULTILINESTRING ((1 1, 2 2, 3 3, 1 1), (1 0, 2 0, 3 0))');
SELECT toTypeName(readWKTMultiLineString('MULTILINESTRING ((1 1, 2 2, 3 3, 1 1), (1 0, 2 0, 3 0))'));
SELECT wkt(readWKTMultiLineString('MULTILINESTRING ((1 1, 2 2, 3 3, 1 1), (1 0, 2 0, 3 0))'));

-- Native Array(Array(Tuple(Float64, Float64))) is treated as Polygon, not as MultiLineString.
WITH wkt(CAST([[(1, 1), (2, 2), (3, 3), (1, 1)]], 'Array(Array(Tuple(Float64, Float64)))')) as x
SELECT x, toTypeName(x), readWKTPolygon(x) as y, toTypeName(y);

-- Non constant tests

CREATE TABLE IF NOT EXISTS t (shape Array(Array(Tuple(Float64, Float64))), wkt_string String) Engine = Memory;
INSERT INTO t (shape, wkt_string) VALUES ([[(1, 1), (2, 2), (3, 3), (1, 1)]], 'MULTILINESTRING ((1 1, 2 2, 3 3, 1 1))');
INSERT INTO t (shape, wkt_string) VALUES ([[(1, 1), (2, 2), (3, 3), (1, 1)], [(1, 0), (2, 0), (3, 0)]], 'MULTILINESTRING ((1 1, 2 2, 3 3, 1 1), (1 0, 2 0, 3 0))');

-- Native Array(Array(Tuple(Float64, Float64))) is treated as Polygon, not as MultiLineString.
-- but reading MultiLineString should still return an Array(Array(Tuple(Float64, Float64)))
select wkt(shape), readWKTMultiLineString(wkt_string), readWKTMultiLineString(wkt_string) = shape from t;



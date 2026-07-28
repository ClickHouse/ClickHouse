-- readWKT must accept leading whitespace before the geometry type, matching the
-- typed readWKT* readers and the WKT grammar (issue #110700).
SELECT readWKT('  POINT(1 2)');
SELECT readWKT('\tLINESTRING(1 1, 2 2)');
SELECT readWKT('  POLYGON((1 0,10 0,10 10,0 10,1 0))');
SELECT readWKT(' MULTILINESTRING((1 1, 2 2), (3 3, 4 4))');
SELECT readWKT('  MULTIPOLYGON(((2 0,10 0,10 10,0 10,2 0)))');

-- Consistent with the typed reader on the same whitespace-prefixed input.
SELECT readWKT('  POINT(1 2)') = readWKTPoint('  POINT(1 2)');

-- Trailing whitespace after the geometry value is also accepted (the WKT grammar
-- skips it), consistently between readWKT and the typed readers.
SELECT readWKT('POINT(1 2)   ');
SELECT readWKT('LINESTRING(1 1, 2 2)\t');
SELECT readWKT('  POINT(1 2)   ');
SELECT readWKT('POINT(1 2)   ') = readWKTPoint('POINT(1 2)   ');

-- Tags: no-fasttest

-- Test WKT uppercase alias
SELECT WKT((1.0, 2.0));
SELECT WKT(readWKTPoint('POINT (3 4)'));

-- Test WKB uppercase alias
SELECT hex(WKB(CAST((0, 0), 'Point')));

-- Test roundtrip: WKT -> readWKT -> WKT
SELECT WKT(readWKT('LINESTRING (1 1, 2 2, 3 3, 1 1)'));

-- Test roundtrip: WKB -> readWKB -> WKB
SELECT hex(WKB(readWKBPoint(unhex('0101000000333333333333f33f3333333333330b40'))));

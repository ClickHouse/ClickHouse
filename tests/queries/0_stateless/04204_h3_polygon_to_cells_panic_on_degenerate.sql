-- Tags: no-fasttest
-- Regression test for `h3PolygonToCells` panic on degenerate polygons.
-- The AST fuzzer produced a polygon whose coordinates triggered a panic
-- in the line-sweep intersection algorithm of `geo` (a transitive dep of
-- `h3o`). Because the FFI wrapper used `extern "C"`, the panic could not
-- unwind across the C ABI boundary and the server aborted with signal 6.
-- The fix wraps polygon operations in `catch_unwind` and converts panics
-- into `E_FAILED`, which surfaces as an empty result instead of an abort.
-- See: https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=100272&sha=e96a06161537e78ab2d2bb73bae82a79aa845c49&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%2C%20targeted%2C%20old_compatibility%29

-- The original fuzzer query (simplified to the polygon part) used to abort the server.
-- Now it should either return some result or an empty array, but must not crash.
SELECT length(h3PolygonToCells(
    [(100.0001, 1.1754943508222875e-38),
     (100000000000000000000., 3.4028234663852886e38),
     (1000.0001, 100.0001),
     (0.0001, 100.0001)],
    2)) >= 0
FORMAT Null;

-- A few more degenerate inputs that exercise the same code path.
SELECT length(h3PolygonToCells([(0.0, 0.0), (0.0, 0.0), (0.0, 0.0)], 5)) >= 0 FORMAT Null;
SELECT length(h3PolygonToCells([(1e300, 1e300), (-1e300, -1e300), (1e300, -1e300)], 7)) >= 0 FORMAT Null;

-- NaN coordinates are rejected by the geometry input validator with a
-- controlled exception (not a panic), so this case never reaches `h3o`.
SELECT length(h3PolygonToCells([(nan, nan), (nan, nan), (nan, nan)], 3)) >= 0 FORMAT Null; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

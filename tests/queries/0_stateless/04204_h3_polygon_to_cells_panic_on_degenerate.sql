-- Regression test for `h3PolygonToCells` panic on degenerate polygons.
-- The AST fuzzer produced a polygon whose coordinates triggered a panic
-- in the line-sweep intersection algorithm of `geo` (a transitive dep of
-- `h3o`). Because the FFI wrapper used `extern "C"`, the panic could not
-- unwind across the C ABI boundary and the server aborted with signal 6.
-- The fix wraps polygon operations in `catch_unwind` and converts panics
-- into `E_FAILED`; the C++ caller then surfaces that as a controlled
-- `BAD_ARGUMENTS` exception instead of an abort or a silent empty result.
-- See: https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=100272&sha=e96a06161537e78ab2d2bb73bae82a79aa845c49&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%2C%20targeted%2C%20old_compatibility%29
-- Upstream report: https://github.com/HydroniumLabs/h3o/issues/44

-- The original fuzzer polygon used to abort the server.
SELECT length(h3PolygonToCells(
    [(100.0001, 1.1754943508222875e-38),
     (100000000000000000000., 3.4028234663852886e38),
     (1000.0001, 100.0001),
     (0.0001, 100.0001)],
    2))
FORMAT Null; -- { serverError BAD_ARGUMENTS }

-- NaN coordinates are rejected by the geometry input validator with a
-- controlled exception (not a panic), so this case never reaches `h3o`.
SELECT length(h3PolygonToCells([(nan, nan), (nan, nan), (nan, nan)], 3)) >= 0 FORMAT Null; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

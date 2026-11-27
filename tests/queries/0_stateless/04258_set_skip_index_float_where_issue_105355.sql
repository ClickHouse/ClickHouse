-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105355
-- `__bitWrapperFunc` only supports integer arguments. The set skip index used
-- to wrap any WHERE atom (including atoms whose result type is `Float`) with
-- `__bitWrapperFunc`, which threw the internal "It's a bug!" exception at
-- execution time. After the fix, non-integer atom result types fall back to
-- `UNKNOWN_FIELD` and the query goes through the regular filter path.

DROP TABLE IF EXISTS t_105355_uint;
DROP TABLE IF EXISTS t_105355_float;

CREATE TABLE t_105355_uint (c0 UInt32, INDEX idx c0 TYPE set(100) GRANULARITY 4)
    ENGINE = MergeTree() ORDER BY c0;
INSERT INTO t_105355_uint VALUES (2);

-- Float64 atom from arithmetic
SELECT c0 FROM t_105355_uint WHERE c0 + 0.1;

-- Float64 atom from `log` (non-zero result)
SELECT c0 FROM t_105355_uint WHERE log(c0);

-- Float64 atom from negation of `log`
SELECT c0 FROM t_105355_uint WHERE -log(c0);

-- BFloat16 atom
SELECT c0 FROM t_105355_uint WHERE c0 + toBFloat16(0.1);

-- Float column as a direct WHERE atom (also reachable from an INPUT)
CREATE TABLE t_105355_float (f Float32, INDEX idx f TYPE set(100) GRANULARITY 4)
    ENGINE = MergeTree() ORDER BY f;
INSERT INTO t_105355_float VALUES (1.5);
SELECT f FROM t_105355_float WHERE f;

-- Sanity check: integer atoms still use the index and return correct results
SELECT c0 FROM t_105355_uint WHERE c0 = 2;
SELECT c0 FROM t_105355_uint WHERE c0 > 0;

DROP TABLE t_105355_uint;
DROP TABLE t_105355_float;

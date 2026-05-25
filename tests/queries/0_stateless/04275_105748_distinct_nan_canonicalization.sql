-- Regression for https://github.com/ClickHouse/ClickHouse/issues/105748
-- Different operations and platforms can produce NaN values with different
-- bit patterns (sign bit, payload). ClickHouse hash tables compare keys
-- byte-wise, so without canonicalization `DISTINCT`, `GROUP BY`, `uniqExact`
-- treat such NaNs as separate values. Concretely:
--   * `0./0.` produces 0xFFF8000000000000 on x86 (constant-folded)
--   * The literal `nan` is 0x7FF8000000000000
--   * After PR #98230, ARM NEON `log(-1.)` returns 0xFFF8000000000000
--     whereas glibc's scalar `log(-1.)` returns 0x7FF8000000000000
-- All of these must be deduplicated to a single NaN value.

-- countDistinct / uniqExact

SELECT 'countDistinct(div0, log(-1))', countDistinct(arrayJoin([0./0., log(-1.)]));
SELECT 'countDistinct(div0, nan)', countDistinct(arrayJoin([0./0., nan]));
SELECT 'countDistinct(div0, nan, log(-1))', countDistinct(arrayJoin([0./0., nan, log(-1.)]));

SELECT 'uniqExact Float64', uniqExact(arrayJoin([0./0., nan, log(-1.)]));
SELECT 'uniqExact Float32', uniqExact(arrayJoin([toFloat32(0./0.), toFloat32(nan), toFloat32(log(-1.))]));
SELECT 'uniqExact BFloat16', uniqExact(arrayJoin([toBFloat16(0./0.), toBFloat16(nan), toBFloat16(log(-1.))]));

-- HLL-based `uniq` should also see one NaN

SELECT 'uniq Float64', uniq(arrayJoin([0./0., nan, log(-1.)]));

-- SELECT DISTINCT (DistinctTransform, single Float key, `SetVariants::key64`/`key32`/`key16`)

SELECT 'SELECT DISTINCT Float64', count() FROM (
    SELECT DISTINCT x FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x)
);
SELECT 'SELECT DISTINCT Float32', count() FROM (
    SELECT DISTINCT x FROM (SELECT arrayJoin([toFloat32(0./0.), toFloat32(nan), toFloat32(log(-1.))]) AS x)
);
SELECT 'SELECT DISTINCT BFloat16', count() FROM (
    SELECT DISTINCT x FROM (SELECT arrayJoin([toBFloat16(0./0.), toBFloat16(nan), toBFloat16(log(-1.))]) AS x)
);

-- GROUP BY single Float key (`Aggregator::key64`/`key32`/`key16`)

SELECT 'GROUP BY Float64', count() FROM (
    SELECT x FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x) GROUP BY x
);
SELECT 'GROUP BY Float32', count() FROM (
    SELECT x FROM (SELECT arrayJoin([toFloat32(0./0.), toFloat32(nan), toFloat32(log(-1.))]) AS x) GROUP BY x
);
SELECT 'GROUP BY BFloat16', count() FROM (
    SELECT x FROM (SELECT arrayJoin([toBFloat16(0./0.), toBFloat16(nan), toBFloat16(log(-1.))]) AS x) GROUP BY x
);

-- Nullable(Float64)/(BFloat16) for paths that go through `HashMethodOneNumber` (nullable=true):
-- `uniqExact` and `GROUP BY` of a single `Nullable(Float)` key. `SELECT DISTINCT`
-- of a single `Nullable` key currently routes through `SetVariants::nullable_keys128`
-- (HashMethodKeysFixed packing), which is a multi-key path and not covered by this
-- fix; see follow-up note in the PR description.

SELECT 'uniqExact Nullable(Float64)', uniqExact(x) FROM (
    SELECT arrayJoin([toNullable(toFloat64(0./0.)), toNullable(toFloat64(nan)), toNullable(toFloat64(log(-1.)))]) AS x
);
SELECT 'GROUP BY Nullable(Float64)', count() FROM (
    SELECT x FROM (SELECT arrayJoin([toNullable(toFloat64(0./0.)), toNullable(toFloat64(nan)), toNullable(toFloat64(log(-1.)))]) AS x) GROUP BY x
);
SELECT 'uniqExact Nullable(BFloat16)', uniqExact(x) FROM (
    SELECT arrayJoin([toNullable(toBFloat16(0./0.)), toNullable(toBFloat16(nan)), toNullable(toBFloat16(log(-1.)))]) AS x
);
SELECT 'GROUP BY Nullable(BFloat16)', count() FROM (
    SELECT x FROM (SELECT arrayJoin([toNullable(toBFloat16(0./0.)), toNullable(toBFloat16(nan)), toNullable(toBFloat16(log(-1.)))]) AS x) GROUP BY x
);

-- Real table with materialized NaN values of different bit patterns

DROP TABLE IF EXISTS t_105748_nan;
CREATE TABLE t_105748_nan (x Float64) ENGINE = Memory;
INSERT INTO t_105748_nan VALUES (nan), (0./0.), (log(-1.0)), (sqrt(-1.0));

SELECT 'rows in table', count(*) FROM t_105748_nan;
SELECT 'distinct NaNs in table', uniqExact(x) FROM t_105748_nan;
SELECT 'count rows after DISTINCT', count() FROM (SELECT DISTINCT x FROM t_105748_nan);
SELECT 'count groups after GROUP BY', count() FROM (SELECT x, count() AS c FROM t_105748_nan GROUP BY x);

DROP TABLE t_105748_nan;

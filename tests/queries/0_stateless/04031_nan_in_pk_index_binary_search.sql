-- Tags: no-random-merge-tree-settings

-- Regression test: NaN values in primary key index broke binary search
-- in markRangesFromPKRange because Range::intersectsRange gives wrong
-- results for IEEE 754 NaN (NaN is incomparable).
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=744485a03e0c78561cc7f820e9b43decf2cea69d&name_0=MasterCI&name_1=AST%20fuzzer%20%28amd_debug%29

DROP TABLE IF EXISTS t_nan_pk;

CREATE TABLE t_nan_pk (col Nullable(Float32))
ENGINE = MergeTree ORDER BY col
SETTINGS allow_nullable_key = 1, index_granularity = 1;

INSERT INTO t_nan_pk SELECT arrayJoin([NULL, inf, 2.0, -inf, 3.0, nan, -nan, NULL])::Nullable(Float32);

SELECT count() FROM t_nan_pk WHERE col < 0;
SELECT count() FROM t_nan_pk WHERE col < isNull(nan);
SELECT DISTINCT count() FROM t_nan_pk PREWHERE col < isNull(nan) WHERE col < isNull(nan);

-- Also test with non-nullable Float column
DROP TABLE IF EXISTS t_nan_pk2;

CREATE TABLE t_nan_pk2 (col Float32)
ENGINE = MergeTree ORDER BY col
SETTINGS index_granularity = 1;

INSERT INTO t_nan_pk2 SELECT arrayJoin([inf, 2.0, -inf, 3.0, nan, -nan])::Float32;

SELECT count() FROM t_nan_pk2 WHERE col < 0;
SELECT count() FROM t_nan_pk2 WHERE col < isNull(nan);
SELECT DISTINCT count() FROM t_nan_pk2 PREWHERE col < isNull(nan) WHERE col < isNull(nan);

DROP TABLE t_nan_pk;
DROP TABLE t_nan_pk2;

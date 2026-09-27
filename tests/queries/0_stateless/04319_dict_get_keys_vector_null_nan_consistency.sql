-- The constant path of `dictGetKeys` compares the attribute against the value with SQL `equals`,
-- while the vector path uses a hash to group equal values. For `NULL` and `NaN`, `equals` is not
-- true even though the hashes are equal, so the vector path must confirm candidates with `equals`
-- too. Otherwise `dictGetKeys(dict, attr, materialize(NULL))` would observably differ from
-- `dictGetKeys(dict, attr, NULL)`. This test pins the two paths together.

DROP DICTIONARY IF EXISTS dict_consistency;
DROP TABLE IF EXISTS src_consistency;

CREATE TABLE src_consistency
(
    id   UInt64,
    i32n Nullable(Int32),
    f64  Float64
)
ENGINE = Memory;

INSERT INTO src_consistency VALUES
    (1, 10, 1.5),
    (2, 10, nan),
    (3, -5, 2.5),
    (4, NULL, nan);

CREATE DICTIONARY dict_consistency
(
    id   UInt64,
    i32n Nullable(Int32),
    f64  Float64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src_consistency'))
LIFETIME(0)
LAYOUT(HASHED());

SELECT 'Nullable attribute: constant vs vector';
-- Constant path
SELECT dictGetKeys('dict_consistency', 'i32n', CAST(NULL AS Nullable(Int32)));
SELECT dictGetKeys('dict_consistency', 'i32n', CAST(10 AS Nullable(Int32)));
-- Vector path (materialize forces a non-constant argument)
SELECT dictGetKeys('dict_consistency', 'i32n', materialize(CAST(NULL AS Nullable(Int32))));
SELECT dictGetKeys('dict_consistency', 'i32n', materialize(CAST(10 AS Nullable(Int32))));

SELECT 'Nullable attribute: vector with NULLs interleaved';
SELECT dictGetKeys('dict_consistency', 'i32n', x)
FROM (SELECT arrayJoin([CAST(NULL AS Nullable(Int32)), CAST(10 AS Nullable(Int32)), CAST(NULL AS Nullable(Int32)), CAST(-5 AS Nullable(Int32))]) AS x);

SELECT 'Float NaN: constant vs vector';
-- Constant path
SELECT dictGetKeys('dict_consistency', 'f64', nan);
SELECT dictGetKeys('dict_consistency', 'f64', CAST(2.5 AS Float64));
-- Vector path
SELECT dictGetKeys('dict_consistency', 'f64', materialize(CAST(nan AS Float64)));
SELECT dictGetKeys('dict_consistency', 'f64', materialize(CAST(2.5 AS Float64)));

SELECT 'Float NaN: vector with NaNs interleaved';
SELECT dictGetKeys('dict_consistency', 'f64', x)
FROM (SELECT arrayJoin([CAST(nan AS Float64), CAST(1.5 AS Float64), CAST(nan AS Float64), CAST(2.5 AS Float64)]) AS x);

SELECT 'Constant path equals vector path for every distinct value (must all be 1)';
SELECT
    dictGetKeys('dict_consistency', 'i32n', x) = dictGetKeys('dict_consistency', 'i32n', materialize(x)) AS i32n_consistent,
    dictGetKeys('dict_consistency', 'f64', y) = dictGetKeys('dict_consistency', 'f64', materialize(y)) AS f64_consistent
FROM
(
    SELECT
        arrayJoin([CAST(NULL AS Nullable(Int32)), CAST(10 AS Nullable(Int32)), CAST(-5 AS Nullable(Int32)), CAST(999 AS Nullable(Int32))]) AS x,
        arrayJoin([CAST(nan AS Float64), CAST(1.5 AS Float64), CAST(2.5 AS Float64), CAST(7.0 AS Float64)]) AS y
);

DROP DICTIONARY dict_consistency;
DROP TABLE src_consistency;

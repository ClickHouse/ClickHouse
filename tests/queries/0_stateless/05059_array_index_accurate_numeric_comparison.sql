-- https://github.com/ClickHouse/ClickHouse/issues/116928
-- `has`/`indexOf`/`countEqual` over an array of numbers compared the elements with a plain C++ `==`
-- after an implicit cast, so for a mismatched numeric pair they disagreed with `equals`, and with
-- their own constant-array path, which uses accurate comparison: `-1` wrapped to the `UInt64` maximum
-- and `16777217` rounded to a `Float32`. The default-on `optimize_rewrite_array_exists_to_has` then
-- turned a correct `arrayExists(x -> x = c, arr)` into a `has` that returns phantom rows.

SELECT 'ground truth';
SELECT toUInt64(18446744073709551615) = -1, toFloat32(16777216) = 16777217;

SELECT 'sign wraparound';
SELECT has([toUInt64(18446744073709551615)], -1), has(materialize([toUInt64(18446744073709551615)]), -1);
SELECT indexOf([toUInt64(18446744073709551615)], -1), indexOf(materialize([toUInt64(18446744073709551615)]), -1);
SELECT countEqual([toUInt64(18446744073709551615)], -1), countEqual(materialize([toUInt64(18446744073709551615)]), -1);

SELECT 'float precision';
SELECT has([toFloat32(16777216)], 16777217), has(materialize([toFloat32(16777216)]), 16777217);
SELECT indexOf([toFloat32(16777216)], 16777217), indexOf(materialize([toFloat32(16777216)]), 16777217);

SELECT 'arrayExists rewrite';
DROP TABLE IF EXISTS t_array_index_accurate;
CREATE TABLE t_array_index_accurate (id UInt8, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_array_index_accurate VALUES (1, [18446744073709551615]), (2, [5]);
SELECT id FROM t_array_index_accurate WHERE arrayExists(x -> x = -1, arr) ORDER BY id;
SELECT id FROM t_array_index_accurate WHERE arrayExists(x -> x = -1, arr) ORDER BY id SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT id FROM t_array_index_accurate WHERE has(arr, -1) ORDER BY id;

DROP TABLE IF EXISTS t_array_index_accurate_float;
CREATE TABLE t_array_index_accurate_float (id UInt8, arr Array(Float32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_array_index_accurate_float VALUES (1, [16777216.0]), (2, [1.5]);
SELECT id FROM t_array_index_accurate_float WHERE arrayExists(x -> x = 16777217, arr) ORDER BY id;
SELECT id FROM t_array_index_accurate_float WHERE arrayExists(x -> x = 16777217, arr) ORDER BY id SETTINGS optimize_rewrite_array_exists_to_has = 0;

SELECT 'matching pairs still match';
SELECT has(materialize([toUInt64(5)]), 5), has(materialize([toInt64(-1)]), -1), has(materialize([toFloat64(1.5)]), 1.5);
SELECT has(materialize([toUInt64(5)]), toInt32(5)), has(materialize([toFloat32(1.5)]), 1.5);
SELECT indexOf(materialize([toUInt64(1), toUInt64(2), toUInt64(3)]), 2);
SELECT has(materialize([nan]), nan), has([nan], nan);
SELECT id FROM t_array_index_accurate WHERE has(arr, toUInt64(5)) ORDER BY id;

SELECT 'LowCardinality dictionary path';
-- The dictionary fast path resolves the constant needle with a plain narrowing cast, so it has to
-- answer from a dictionary entry only when the constant survived that cast.
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS t_array_index_accurate_lc;
CREATE TABLE t_array_index_accurate_lc
(
    id UInt8,
    a8 Array(LowCardinality(UInt8)),
    a64 Array(LowCardinality(UInt64)),
    af32 Array(LowCardinality(Float32))
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_array_index_accurate_lc VALUES (1, [255], [18446744073709551615], [16777216]), (2, [5], [5], [5]);
SELECT id FROM t_array_index_accurate_lc WHERE has(a8, -1) ORDER BY id;
SELECT id FROM t_array_index_accurate_lc WHERE has(a64, -1) ORDER BY id;
SELECT id FROM t_array_index_accurate_lc WHERE has(af32, 16777217) ORDER BY id;
SELECT id FROM t_array_index_accurate_lc WHERE indexOf(a8, -1) > 0 ORDER BY id;
SELECT sum(countEqual(a8, -1)) FROM t_array_index_accurate_lc;
SELECT id FROM t_array_index_accurate_lc WHERE arrayExists(x -> x = -1, a8) ORDER BY id;
SELECT id FROM t_array_index_accurate_lc WHERE arrayExists(x -> x = -1, a8) ORDER BY id SETTINGS optimize_rewrite_array_exists_to_has = 0;

SELECT 'LowCardinality matching pairs still match';
SELECT id FROM t_array_index_accurate_lc WHERE has(a8, 5) ORDER BY id;
SELECT id FROM t_array_index_accurate_lc WHERE has(a8, toInt32(255)) ORDER BY id;
SELECT id FROM t_array_index_accurate_lc WHERE has(a64, 5) ORDER BY id;
SELECT id FROM t_array_index_accurate_lc WHERE has(af32, 5) ORDER BY id;
SELECT id, indexOf(a8, 255), countEqual(a64, 18446744073709551615) FROM t_array_index_accurate_lc ORDER BY id;

SELECT 'LowCardinality strings keep their padding semantics';
SELECT has(materialize(CAST(['ab'], 'Array(LowCardinality(FixedString(3)))')), 'ab');
SELECT has(materialize(CAST(['ab'], 'Array(LowCardinality(String))')), 'ab');
SELECT has(materialize(CAST(['o'], 'Array(LowCardinality(FixedString(3)))')), CAST('o', 'Enum8(\'\' = 0, \'o\' = 1)'));

SELECT 'LowCardinality lossy casts outside the numeric domain';
-- https://github.com/ClickHouse/ClickHouse/issues/117316
-- A cast loses just as silently outside the numeric domain, and the slot its image lands on says
-- nothing about whether the constant survived it, so a needle that did not survive must be declined
-- wherever it lands. The timezone is pinned because the cast to Date drops the needle's time of day
-- and which day that lands on is offset-dependent. Every row prints the plain-array oracle beside it.
SELECT has(materialize(CAST([toDate('2020-01-01')], 'Array(LowCardinality(Date))')), toDateTime('2020-01-01 00:00:05')) AS lc, has(materialize(CAST([toDate('2020-01-01')], 'Array(Date)')), toDateTime('2020-01-01 00:00:05')) AS oracle SETTINGS session_timezone = 'UTC';
SELECT indexOf(materialize(CAST([toDate('2020-01-01')], 'Array(LowCardinality(Date))')), toDateTime('2020-01-01 00:00:05')) AS lc, indexOf(materialize(CAST([toDate('2020-01-01')], 'Array(Date)')), toDateTime('2020-01-01 00:00:05')) AS oracle SETTINGS session_timezone = 'UTC';
SELECT countEqual(materialize(CAST([toDate('2020-01-01')], 'Array(LowCardinality(Date))')), toDateTime('2020-01-01 00:00:05')) AS lc, countEqual(materialize(CAST([toDate('2020-01-01')], 'Array(Date)')), toDateTime('2020-01-01 00:00:05')) AS oracle SETTINGS session_timezone = 'UTC';
-- A Date32 needle that the Date element represents exactly is still found, on a slot that is not the
-- default one.
SELECT has(materialize(CAST([toDate('2020-01-01')], 'Array(LowCardinality(Date))')), toDate32('2020-01-01')) AS lc, has(materialize(CAST([toDate('2020-01-01')], 'Array(Date)')), toDate32('2020-01-01')) AS oracle;

SELECT 'LowCardinality NULL needle';
-- Slot 0 is the NULL value only in a nullable dictionary; in a non-nullable one it holds the nested
-- default value, which no NULL needle equals.
SELECT has(materialize(CAST(['', 'a'], 'Array(LowCardinality(String))')), NULL) AS lc, has(materialize(CAST(['', 'a'], 'Array(String)')), NULL) AS oracle;
SELECT indexOf(materialize(CAST(['', 'a'], 'Array(LowCardinality(String))')), NULL) AS lc, indexOf(materialize(CAST(['', 'a'], 'Array(String)')), NULL) AS oracle;
SELECT countEqual(materialize(CAST(['', 'a', ''], 'Array(LowCardinality(String))')), NULL) AS lc, countEqual(materialize(CAST(['', 'a', ''], 'Array(String)')), NULL) AS oracle;
SELECT has(materialize(CAST([0, 5], 'Array(LowCardinality(UInt8))')), NULL) AS lc, has(materialize(CAST([0, 5], 'Array(UInt8)')), NULL) AS oracle;
-- A nullable dictionary still finds its NULL element, and a non-NULL needle still finds its own.
SELECT indexOf(materialize(CAST(['a', NULL, ''], 'Array(LowCardinality(Nullable(String)))')), NULL) AS lc, indexOf(materialize(CAST(['a', NULL, ''], 'Array(Nullable(String))')), NULL) AS oracle;
SELECT countEqual(materialize(CAST([NULL, 'a', NULL], 'Array(LowCardinality(Nullable(String)))')), NULL) AS lc, countEqual(materialize(CAST([NULL, 'a', NULL], 'Array(Nullable(String))')), NULL) AS oracle;
SELECT indexOf(materialize(CAST(['a', NULL, ''], 'Array(LowCardinality(Nullable(String)))')), '') AS lc, indexOf(materialize(CAST(['a', NULL, ''], 'Array(Nullable(String))')), '') AS oracle;

SELECT 'a needle counting another unit is compared in the type the pair meets in';
-- https://github.com/ClickHouse/ClickHouse/pull/117313#discussion_r3936389715
-- A `Date` counts days and a `DateTime` counts seconds since the epoch, and both paths that compare
-- values read the two as the raw numbers they are stored as -- 19723 days against 1704067200 seconds
-- -- so neither found `toDateTime('2024-01-01 00:00:00')` in an array holding that same instant,
-- while `equals` and `IN` both consider the pair equal, and so did the `LowCardinality` encoding of
-- the same haystack, which resolves the needle by a cast. Every encoding now answers in the type the
-- pair meets in, which is the type `equals` compares them in.
-- The timezone is pinned because it is randomized in CI and it decides which instant the needle is.
SET session_timezone = 'UTC';
SELECT toDate('2024-01-01') = toDateTime('2024-01-01 00:00:00') AS equals, toDateTime('2024-01-01 00:00:00') IN (toDate('2024-01-01')) AS in_operator;
SELECT has(CAST([toDate('2024-01-01')], 'Array(Date)'), toDateTime('2024-01-01 00:00:00')) AS const, has(materialize(CAST([toDate('2024-01-01')], 'Array(Date)')), toDateTime('2024-01-01 00:00:00')) AS materialized, has(materialize(CAST([toDate('2024-01-01')], 'Array(LowCardinality(Date))')), toDateTime('2024-01-01 00:00:00')) AS low_cardinality;
SELECT indexOf(CAST([toDate('2024-01-02'), toDate('2024-01-01')], 'Array(Date)'), toDateTime('2024-01-01 00:00:00')) AS const, indexOf(materialize(CAST([toDate('2024-01-02'), toDate('2024-01-01')], 'Array(Date)')), toDateTime('2024-01-01 00:00:00')) AS materialized, indexOf(materialize(CAST([toDate('2024-01-02'), toDate('2024-01-01')], 'Array(LowCardinality(Date))')), toDateTime('2024-01-01 00:00:00')) AS low_cardinality;
SELECT countEqual(CAST([toDate('2024-01-01'), toDate('2024-01-02'), toDate('2024-01-01')], 'Array(Date)'), toDateTime('2024-01-01 00:00:00')) AS const, countEqual(materialize(CAST([toDate('2024-01-01'), toDate('2024-01-02'), toDate('2024-01-01')], 'Array(Date)')), toDateTime('2024-01-01 00:00:00')) AS materialized, countEqual(materialize(CAST([toDate('2024-01-01'), toDate('2024-01-02'), toDate('2024-01-01')], 'Array(LowCardinality(Date))')), toDateTime('2024-01-01 00:00:00')) AS low_cardinality;
-- A `Date32` element counts days too, and so does a `Date32` needle over a `Date` element.
SELECT has(CAST([toDate32('2024-01-01')], 'Array(Date32)'), toDateTime('2024-01-01 00:00:00')) AS const, has(materialize(CAST([toDate32('2024-01-01')], 'Array(Date32)')), toDateTime('2024-01-01 00:00:00')) AS materialized, has(materialize(CAST([toDate32('2024-01-01')], 'Array(LowCardinality(Date32))')), toDateTime('2024-01-01 00:00:00')) AS low_cardinality;
SELECT countEqual(CAST([toDate32('2024-01-01'), toDate32('2024-01-02'), toDate32('2024-01-01')], 'Array(Date32)'), toDateTime('2024-01-01 00:00:00')) AS const, countEqual(materialize(CAST([toDate32('2024-01-01'), toDate32('2024-01-02'), toDate32('2024-01-01')], 'Array(Date32)')), toDateTime('2024-01-01 00:00:00')) AS materialized, countEqual(materialize(CAST([toDate32('2024-01-01'), toDate32('2024-01-02'), toDate32('2024-01-01')], 'Array(LowCardinality(Date32))')), toDateTime('2024-01-01 00:00:00')) AS low_cardinality;
SELECT indexOf(CAST([toDate('2024-01-02'), toDate('2024-01-01')], 'Array(Date)'), toDate32('2024-01-01')) AS const, indexOf(materialize(CAST([toDate('2024-01-02'), toDate('2024-01-01')], 'Array(Date)')), toDate32('2024-01-01')) AS materialized, indexOf(materialize(CAST([toDate('2024-01-02'), toDate('2024-01-01')], 'Array(LowCardinality(Date))')), toDate32('2024-01-01')) AS low_cardinality;
-- The other direction: a `Date` needle over a `DateTime` element, which the pair meets as `DateTime`.
SELECT has(CAST([toDateTime('2024-01-01 00:00:00')], 'Array(DateTime)'), toDate('2024-01-01')) AS const, has(materialize(CAST([toDateTime('2024-01-01 00:00:00')], 'Array(DateTime)')), toDate('2024-01-01')) AS materialized, has(materialize(CAST([toDateTime('2024-01-01 00:00:00')], 'Array(LowCardinality(DateTime))')), toDate('2024-01-01')) AS low_cardinality;
SELECT has(CAST([toDateTime('2024-01-01 00:00:00')], 'Array(DateTime)'), toDateTime64('2024-01-01 00:00:00', 3)) AS const, has(materialize(CAST([toDateTime('2024-01-01 00:00:00')], 'Array(DateTime)')), toDateTime64('2024-01-01 00:00:00', 3)) AS materialized, has(materialize(CAST([toDateTime('2024-01-01 00:00:00')], 'Array(LowCardinality(DateTime))')), toDateTime64('2024-01-01 00:00:00', 3)) AS low_cardinality;
-- A non-constant needle skips the dictionary shortcut in both encodings.
SELECT has(materialize(CAST([toDate('2024-01-01')], 'Array(Date)')), materialize(toDateTime('2024-01-01 00:00:00'))) AS plain, has(materialize(CAST([toDate('2024-01-01')], 'Array(LowCardinality(Date))')), materialize(toDateTime('2024-01-01 00:00:00'))) AS low_cardinality;
-- A needle that the element does not represent is still found nowhere: the pair meets as `DateTime`,
-- where the element is midnight and the needle is not.
SELECT toDate('2024-01-01') = toDateTime('2024-01-01 00:00:05') AS equals, has(CAST([toDate('2024-01-01')], 'Array(Date)'), toDateTime('2024-01-01 00:00:05')) AS const, has(materialize(CAST([toDate('2024-01-01')], 'Array(Date)')), toDateTime('2024-01-01 00:00:05')) AS materialized, has(materialize(CAST([toDate('2024-01-01')], 'Array(LowCardinality(Date))')), toDateTime('2024-01-01 00:00:05')) AS low_cardinality;
-- A NULL element of a nullable array is still not the needle, and a NULL needle still finds nothing.
SELECT has(materialize(CAST([toDate('2024-01-01'), NULL], 'Array(Nullable(Date))')), toDateTime('2024-01-01 00:00:00')) AS materialized, has(materialize(CAST([toDate('2024-01-01'), NULL], 'Array(Nullable(Date))')), CAST(NULL, 'Nullable(DateTime)')) AS null_needle;

DROP TABLE t_array_index_accurate;
DROP TABLE t_array_index_accurate_float;
DROP TABLE t_array_index_accurate_lc;

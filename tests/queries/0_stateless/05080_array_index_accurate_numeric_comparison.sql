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

DROP TABLE t_array_index_accurate;
DROP TABLE t_array_index_accurate_float;

-- Negative zero is equal to positive zero, but it has a different binary representation.
-- Hash tables compare floating point keys bitwise, so negative zero has to be canonicalized
-- to positive zero, otherwise the operations based on hashing disagree with the `equals` function.
-- https://github.com/ClickHouse/ClickHouse/issues/65316

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0 (c0 Int32) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t1 (c0 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t0 VALUES (1);
INSERT INTO t1 VALUES (0);

SELECT 'the original report';
SELECT ((t0.c0 NOT IN (true)) = (t1.c0 / -1)) FROM t1, t0;
SELECT * FROM t1, t0 WHERE ((t0.c0 NOT IN (true)) = (t1.c0 / -1));

DROP TABLE t0;
DROP TABLE t1;

SELECT 'equals';
SELECT materialize(0.0::Float64) = materialize(-0.0::Float64), materialize(0.0::Float32) = materialize(-0.0::Float32);

SELECT 'number of groups';
SELECT count() FROM (SELECT arrayJoin([0.0::Float64, -0.0::Float64]) AS x GROUP BY x);
SELECT count() FROM (SELECT arrayJoin([0.0::Float32, -0.0::Float32]) AS x GROUP BY x);
SELECT count() FROM (SELECT arrayJoin([0.0::BFloat16, -0.0::BFloat16]) AS x GROUP BY x);
SELECT count() FROM (SELECT arrayJoin([0.0::Nullable(Float64), -0.0::Nullable(Float64)]) AS x GROUP BY x);
SELECT count() FROM (SELECT arrayJoin([[0.0::Float64], [-0.0::Float64]]) AS x GROUP BY x);
SELECT count() FROM (SELECT arrayJoin([0.0::Float64, -0.0::Float64]) AS x, 1::UInt32 AS y GROUP BY x, y);
SELECT count() FROM (SELECT arrayJoin([0.0::Float64, -0.0::Float64]) AS x, 'hello' AS y GROUP BY x, y);
SELECT count() FROM (SELECT arrayJoin([0.0::Float64, -0.0::Float64])::LowCardinality(Float64) AS x GROUP BY x) SETTINGS allow_suspicious_low_cardinality_types = 1;

SELECT 'the key is canonicalized, so it is printed as a positive zero';
SELECT x FROM (SELECT arrayJoin([-0.0::Float64]) AS x) GROUP BY x;

SELECT 'distinct';
SELECT count() FROM (SELECT DISTINCT arrayJoin([0.0::Float64, -0.0::Float64]) AS x);

SELECT 'uniq';
SELECT uniqExact(x), uniq(x), uniqHLL12(x), uniqCombined(x) FROM (SELECT arrayJoin([0.0::Float64, -0.0::Float64]) AS x);

SELECT 'in';
SELECT materialize(-0.0::Float64) IN (0.0::Float64), materialize(0.0::Float64) IN (-0.0::Float64);
SELECT materialize((-0.0::Float64, 1)) IN ((0.0::Float64, 1));

SELECT 'join';
SELECT count() FROM (SELECT 0.0::Float64 AS x) AS a JOIN (SELECT -0.0::Float64 AS x) AS b USING (x);
SELECT count() FROM (SELECT 0.0::Float64 AS x, 1::UInt32 AS y) AS a JOIN (SELECT -0.0::Float64 AS x, 1::UInt32 AS y) AS b USING (x, y);
SELECT count() FROM (SELECT 0.0::Float64 AS x, 'hello' AS y) AS a JOIN (SELECT -0.0::Float64 AS x, 'hello' AS y) AS b USING (x, y);
SELECT count() FROM (SELECT 0.0::Float64 AS x) AS a JOIN (SELECT -0.0::Float64 AS x) AS b USING (x) SETTINGS join_algorithm = 'parallel_hash';
SELECT count() FROM (SELECT 0.0::Float64 AS x) AS a JOIN (SELECT -0.0::Float64 AS x) AS b USING (x) SETTINGS join_algorithm = 'grace_hash';
SELECT count() FROM (SELECT 0.0::Float64 AS x) AS a JOIN (SELECT -0.0::Float64 AS x) AS b USING (x) SETTINGS join_algorithm = 'full_sorting_merge';

SELECT 'the joined values are taken from the source blocks, so negative zero is preserved';
SELECT b.y FROM (SELECT materialize(0.0::Float64) AS x) AS a JOIN (SELECT materialize(-0.0::Float64) AS x, materialize(-0.0::Float64) AS y) AS b ON a.x = b.x;

SELECT 'arrays';
SELECT arrayUniq([0.0::Float64, -0.0::Float64]), arrayEnumerateUniq([0.0::Float64, -0.0::Float64]);
SELECT arrayDistinct([0.0::Float64, -0.0::Float64]), arrayCompact([0.0::Float64, -0.0::Float64]);

SELECT 'uniq with multiple arguments and tuples';
SELECT uniqExact(x, y), uniq(x, y), uniqHLL12(x, y), uniqCombined(x, y) FROM (SELECT arrayJoin([0.0::Float64, -0.0::Float64]) AS x, 1::UInt32 AS y);
SELECT uniqExact((x, y)), uniq((x, y)) FROM (SELECT arrayJoin([0.0::Float64, -0.0::Float64]) AS x, 1::UInt32 AS y);
SELECT uniqExact(x, y), uniq(x, y) FROM (SELECT arrayJoin([0.0::Float32, -0.0::Float32]) AS x, 'hello' AS y);
SELECT uniqExact(x, y), uniq(x, y) FROM (SELECT arrayJoin([0.0::BFloat16, -0.0::BFloat16]) AS x, 1::UInt32 AS y);

SELECT 'arrayDistinct of composite elements';
SELECT arrayDistinct([(0.0::Float64, 1), (-0.0::Float64, 1)]);
SELECT arrayDistinct([[0.0::Float64], [-0.0::Float64]]);
SELECT arrayDistinct([0.0::BFloat16, -0.0::BFloat16]);

SELECT 'bloom filter does not skip granules with zeros of the other sign';
DROP TABLE IF EXISTS t_bloom_zero;
CREATE TABLE t_bloom_zero (x Float64, y Float32, arr Array(Float64),
    INDEX ix x TYPE bloom_filter GRANULARITY 1,
    INDEX iy y TYPE bloom_filter GRANULARITY 1,
    INDEX iarr arr TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bloom_zero VALUES (-0.0, -0.0, [-0.0]), (1.5, 1.5, [2.5]);
SELECT count() FROM t_bloom_zero WHERE x = 0.0;
SELECT count() FROM t_bloom_zero WHERE x = -0.0;
SELECT count() FROM t_bloom_zero WHERE y = 0.0;
SELECT count() FROM t_bloom_zero WHERE x IN (0.0, 42.0);
SELECT count() FROM t_bloom_zero WHERE has(arr, 0.0);
SELECT count() FROM t_bloom_zero WHERE hasAny(arr, [0.0]);
SELECT count() FROM t_bloom_zero WHERE hasAll(arr, [0.0]);
DROP TABLE t_bloom_zero;

SELECT 'nan is not equal to zero and is still grouped with itself';
SELECT nan = nan, nan = 0.0, nan = -0.0;
SELECT count() FROM (SELECT arrayJoin([nan, nan, 0.0, -0.0]) AS x GROUP BY x);

SELECT 'variant, dynamic and json keys';
SELECT count() FROM (SELECT arrayJoin([CAST(0.0, 'Variant(Float64, String)'), CAST(-0.0, 'Variant(Float64, String)')]) AS x GROUP BY x) SETTINGS allow_suspicious_types_in_group_by = 1;
SELECT count() FROM (SELECT DISTINCT arrayJoin([CAST(0.0, 'Variant(Float64, String)'), CAST(-0.0, 'Variant(Float64, String)')]) AS x) SETTINGS allow_suspicious_types_in_group_by = 1;
SELECT count() FROM (SELECT CAST(0.0, 'Variant(Float64, String)') AS x) AS a JOIN (SELECT CAST(-0.0, 'Variant(Float64, String)') AS x) AS b USING (x);
SELECT count() FROM (SELECT arrayJoin([CAST(0.0, 'Dynamic'), CAST(-0.0, 'Dynamic')]) AS x GROUP BY x) SETTINGS allow_suspicious_types_in_group_by = 1;
SELECT count() FROM (SELECT arrayJoin([CAST([0.0], 'Dynamic'), CAST([-0.0], 'Dynamic')]) AS x GROUP BY x) SETTINGS allow_suspicious_types_in_group_by = 1;
SELECT count() FROM (SELECT arrayJoin(['{"a":0.0}'::JSON, '{"a":-0.0}'::JSON]) AS x GROUP BY x) SETTINGS allow_suspicious_types_in_group_by = 1;
SELECT count() FROM (SELECT arrayJoin(['{"a":0.0}'::JSON(a Float64), '{"a":-0.0}'::JSON(a Float64)]) AS x GROUP BY x) SETTINGS allow_suspicious_types_in_group_by = 1;

SELECT 'the values in the shared variant of a Dynamic column and in the shared data of a JSON column are stored in binary format';
SELECT count() FROM (SELECT arrayJoin([CAST(0.0, 'Dynamic(max_types=0)'), CAST(-0.0, 'Dynamic(max_types=0)')]) AS x GROUP BY x) SETTINGS allow_suspicious_types_in_group_by = 1;
SELECT count() FROM (SELECT arrayJoin(['{"a":0.0}'::JSON(max_dynamic_paths=0), '{"a":-0.0}'::JSON(max_dynamic_paths=0)]) AS x GROUP BY x) SETTINGS allow_suspicious_types_in_group_by = 1;

SELECT 'map keys';
SELECT count() FROM (SELECT arrayJoin([map(0.0::Float64, 1), map(-0.0::Float64, 1)]) AS x GROUP BY x);

SELECT 'aggregate functions that deduplicate values by their serialized representation';
SELECT groupUniqArray(x) FROM (SELECT arrayJoin([[0.0::Float64], [-0.0::Float64]]) AS x);
SELECT groupUniqArray(x) FROM (SELECT arrayJoin([[0.0::Float32], [-0.0::Float32]]) AS x);
SELECT groupUniqArray(x) FROM (SELECT arrayJoin([(0.0::Float64, 1), (-0.0::Float64, 1)]) AS x);
SELECT groupUniqArray(x) FROM (SELECT arrayJoin([0.0::Nullable(Float64), -0.0::Nullable(Float64)]) AS x);
SELECT groupArrayDistinct(x) FROM (SELECT arrayJoin([[0.0::Float64], [-0.0::Float64]]) AS x);
SELECT uniqExactDistinct(x, materialize(1)) FROM (SELECT arrayJoin([[0.0::Float64], [-0.0::Float64]]) AS x);
SELECT topK(2)(x), topKWeighted(2)(x, 1) FROM (SELECT arrayJoin([[0.0::Float64], [-0.0::Float64]]) AS x);
SELECT groupArrayIntersect(x) FROM (SELECT arrayJoin([[[0.0::Float64]], [[-0.0::Float64]]]) AS x);
SELECT groupArrayIntersect(x) FROM (SELECT arrayJoin([[0.0::Float64], [-0.0::Float64]]) AS x);
SELECT arrayIntersect([[0.0::Float64]], [[-0.0::Float64]]), arrayIntersect([(0.0::Float64, 1)], [(-0.0::Float64, 1)]);
SELECT 'the values are not canonicalized when they are not used as a key';
SELECT groupArray(x) FROM (SELECT arrayJoin([[0.0::Float64], [-0.0::Float64]]) AS x);

SELECT 'values hashed by their generic binary representation';
SELECT arrayUniq([[0.0::Float64], [-0.0::Float64]]), arrayEnumerateUniq([[0.0::Float64], [-0.0::Float64]]), arrayEnumerateDense([[0.0::Float64], [-0.0::Float64]]);
SELECT uniqExact(x), uniq(x), uniqCombined(x), uniqHLL12(x), uniqUpTo(3)(x) FROM (SELECT arrayJoin([[0.0::Float64], [-0.0::Float64]]) AS x);
SELECT materialize([-0.0::Float64]) IN ([0.0::Float64]);

SELECT 'transform';
SELECT transform(materialize(-0.0::Float64), [0.0::Float64], ['zero'], 'other'), transform(materialize(0.0::Float64), [-0.0::Float64], ['zero'], 'other');
SELECT transform(materialize(-0.0::Float64), [0.0::Float64], [42], 0);
SELECT transform(materialize([-0.0::Float64]), [[0.0::Float64]], ['zero'], 'other');
SELECT transform(materialize(2.5::Float64), [2.5::Float64], ['found'], 'other'), transform(materialize(3), [1, 2, 3], ['a', 'b', 'c'], 'other');

SELECT 'the complex key of a dictionary';
DROP DICTIONARY IF EXISTS d_zero;
DROP TABLE IF EXISTS t_dict_zero;
CREATE TABLE t_dict_zero (k Float64, v String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dict_zero VALUES (0.0, 'zero');
CREATE DICTIONARY d_zero (k Float64, v String) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 't_dict_zero' DB currentDatabase()))
LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(MIN 0 MAX 0);
SELECT dictHas('d_zero', tuple(-0.0::Float64)), dictGet('d_zero', 'v', tuple(-0.0::Float64));
DROP DICTIONARY d_zero;
DROP TABLE t_dict_zero;

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

SELECT 'nan is not equal to zero and is still grouped with itself';
SELECT nan = nan, nan = 0.0, nan = -0.0;
SELECT count() FROM (SELECT arrayJoin([nan, nan, 0.0, -0.0]) AS x GROUP BY x);

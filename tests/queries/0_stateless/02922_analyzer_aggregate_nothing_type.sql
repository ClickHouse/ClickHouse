
select sum(NULL);
select quantile(0.5)(NULL);
select quantiles(0.1, 0.2)(NULL :: Nullable(UInt32));
select quantile(0.5)(NULL), quantiles(0.1, 0.2)(NULL :: Nullable(UInt32)), count(NULL), sum(NULL);

SELECT count(NULL) FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY number % 2 WITH TOTALS;
SELECT quantile(0.5)(NULL) FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY number % 2 WITH TOTALS;
SELECT quantiles(0.1, 0.2)(NULL :: Nullable(UInt32)) FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY number % 2 WITH TOTALS;

SELECT '-- notinhgs:';
SELECT nothing() as n, toTypeName(n);
SELECT nothing(1) as n, toTypeName(n);
SELECT nothing(NULL) as n, toTypeName(n);
SELECT nothingUInt64() as n, toTypeName(n);
SELECT nothingUInt64(1) as n, toTypeName(n);
SELECT nothingUInt64(NULL) as n, toTypeName(n);
SELECT nothingNull() as n, toTypeName(n);
SELECT nothingNull(1) as n, toTypeName(n);
SELECT nothingNull(NULL) as n, toTypeName(n);

SELECT '-- quantile:';
SELECT quantileArray(0.5)([NULL, NULL]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT quantileArrayIf(0.5)([NULL], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT quantileArrayIf(0.5)([NULL], 0) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT quantileIfArray(0.5)([NULL, NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT quantileIfArray(0.5)([1, NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT quantileIfArrayIf(0.5)([1, NULL], [1, 0], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT quantileIfArrayArray(0.5)([[1, NULL]], [[1, 0]]) AS x FROM remote('127.0.0.{1,2}', numbers(3));

SELECT '-- quantiles:';
select quantilesArray(0.5, 0.9)([NULL :: Nullable(UInt64), NULL]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT quantilesArrayIf(0.5, 0.9)([NULL :: Nullable(UInt64)], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT quantilesArrayIf(0.5, 0.9)([NULL :: Nullable(UInt64)], 0) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT quantilesIfArray(0.5, 0.9)([NULL :: Nullable(UInt64), NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT quantilesIfArray(0.5, 0.9)([1, NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT quantilesIfArrayIf(0.5, 0.9)([1, NULL], [1, 0], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT quantilesIfArrayArray(0.5, 0.9)([[1, NULL]], [[1, 0]]) AS x FROM remote('127.0.0.{1,2}', numbers(3));

SELECT '-- nothing:';
SELECT nothingArray([NULL, NULL]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingArrayIf([NULL], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingArrayIf([NULL], 0) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingIfArray([NULL, NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingIfArray([1, NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingIfArrayIf([1, NULL], [1, 0], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingIfArrayArray([[1, NULL]], [[1, 0]]) AS x FROM remote('127.0.0.{1,2}', numbers(3));

SELECT '-- nothing(UInt64):';
SELECT nothingUInt64Array([NULL, NULL]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingUInt64ArrayIf([NULL], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingUInt64ArrayIf([NULL], 0) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingUInt64IfArray([NULL, NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingUInt64IfArray([1, NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingUInt64IfArrayIf([1, NULL], [1, 0], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingUInt64IfArrayArray([[1, NULL]], [[1, 0]]) AS x FROM remote('127.0.0.{1,2}', numbers(3));

SELECT '-- nothing(Nullable(Nothing)):';
SELECT nothingNullArray([NULL, NULL]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingNullArrayIf([NULL], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingNullArrayIf([NULL], 0) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingNullIfArray([NULL, NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingNullIfArray([1, NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingNullIfArrayIf([1, NULL], [1, 0], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT nothingNullIfArrayArray([[1, NULL]], [[1, 0]]) AS x FROM remote('127.0.0.{1,2}', numbers(3));

SELECT '-- sum:';
SELECT sumArray([NULL, NULL]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT sumArrayIf([NULL], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT sumArrayIf([NULL], 0) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT sumIfArray([NULL, NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT sumIfArray([1, NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT sumIfArrayIf([1, NULL], [1, 0], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT sumIfArrayArray([[1, NULL]], [[1, 0]]) AS x FROM remote('127.0.0.{1,2}', numbers(3));

SELECT '-- count:';
SELECT countArray([NULL, NULL]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT countArrayIf([NULL], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT countArrayIf([NULL], 0) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT countIfArray([NULL, NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT countIfArray([1, NULL], [1, 0]) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT countIfArrayIf([1, NULL], [1, 0], 1) AS x FROM remote('127.0.0.{1,2}', numbers(3));
SELECT countIfArrayArray([[1, NULL]], [[1, 0]]) AS x FROM remote('127.0.0.{1,2}', numbers(3));


DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (`n` UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t1 SELECT * FROM numbers(10);

SET
enable_parallel_replicas=1,
    max_parallel_replicas=2,
    use_hedged_requests=0,
    cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree=1
;

SELECT count(NULL) FROM t1 WITH TOTALS;
SELECT count(NULL as a), a FROM t1 WITH TOTALS;

SELECT count(NULL as a), sum(a) FROM t1 WITH TOTALS;

SELECT uniq(NULL) FROM t1 WITH TOTALS;
SELECT quantile(0.5)(NULL), quantile(0.9)(NULL), quantiles(0.1, 0.2)(NULL :: Nullable(UInt32)) FROM t1 WITH TOTALS;

CREATE TABLE t (p UInt8, x UInt64) Engine = MergeTree PARTITION BY p ORDER BY x;
INSERT INTO t SELECT 0, number FROM numbers(10);
SELECT count() FROM t WHERE p = 0 AND rowNumberInAllBlocks() = 1 SETTINGS allow_experimental_analyzer = 0;
SELECT count() FROM t WHERE p = 0 AND rowNumberInAllBlocks() = 1 SETTINGS allow_experimental_analyzer = 1;

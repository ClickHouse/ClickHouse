DROP TABLE IF EXISTS test.agg_func_col;

CREATE TABLE test.agg_func_col (p Date, k UInt8, d AggregateFunction(sum, UInt64) DEFAULT arrayReduce('sumState', [toUInt64(200)])) ENGINE = AggregatingMergeTree(p, k, 1);
INSERT INTO test.agg_func_col (k) VALUES (0);
INSERT INTO test.agg_func_col SELECT 1 AS k, arrayReduce('sumState', [toUInt64(100)]) AS d;
SELECT k, sumMerge(d) FROM test.agg_func_col GROUP BY k ORDER BY k;

SELECT '';
ALTER TABLE test.agg_func_col ADD COLUMN af_avg1 AggregateFunction(avg, UInt8);
SELECT k, sumMerge(d), avgMerge(af_avg1) FROM test.agg_func_col GROUP BY k ORDER BY k;

SELECT '';
--INSERT INTO test.agg_func_col (k, af_avg1) VALUES (2, arrayReduce('avgState', [101]));
INSERT INTO test.agg_func_col SELECT 2 AS k, arrayReduce('avgState', [101]) AS af_avg1;
SELECT k, sumMerge(d), avgMerge(af_avg1) FROM test.agg_func_col GROUP BY k ORDER BY k;

SELECT '';
ALTER TABLE test.agg_func_col ADD COLUMN af_gua AggregateFunction(groupUniqArray, String) DEFAULT arrayReduce('groupUniqArrayState', ['---', '---']);
SELECT k, sumMerge(d), avgMerge(af_avg1), groupUniqArrayMerge(af_gua) FROM test.agg_func_col GROUP BY k ORDER BY k;

SELECT '';
INSERT INTO test.agg_func_col SELECT 3 AS k, arrayReduce('avgState', [102, 102]) AS af_avg1, arrayReduce('groupUniqArrayState', ['igua', 'igua']) AS af_gua;
SELECT k, sumMerge(d), avgMerge(af_avg1), groupUniqArrayMerge(af_gua) FROM test.agg_func_col GROUP BY k ORDER BY k;

OPTIMIZE TABLE test.agg_func_col;

SELECT '';
SELECT k, sumMerge(d), avgMerge(af_avg1), groupUniqArrayMerge(af_gua) FROM test.agg_func_col GROUP BY k ORDER BY k;

DROP TABLE IF EXISTS test.agg_func_col;

SELECT '';
SELECT arrayReduce('groupUniqArrayIf', [CAST('---' AS Nullable(String)), CAST('---' AS Nullable(String))], [1, 1])[1];
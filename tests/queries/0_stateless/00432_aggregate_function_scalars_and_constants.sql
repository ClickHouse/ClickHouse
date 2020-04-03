DROP TABLE IF EXISTS agg_func_col;

CREATE TABLE agg_func_col (p Date, k UInt8, d AggregateFunction(sum, UInt64) DEFAULT arrayReduce('sumState', [toUInt64(200)])) ENGINE = AggregatingMergeTree(p, k, 1);
INSERT INTO agg_func_col (k) VALUES (0);
INSERT INTO agg_func_col (k, d) SELECT 1 AS k, arrayReduce('sumState', [toUInt64(100)]) AS d;
SELECT k, sumMerge(d) FROM agg_func_col GROUP BY k ORDER BY k;

SELECT '';
ALTER TABLE agg_func_col ADD COLUMN af_avg1 AggregateFunction(avg, UInt8);
SELECT k, sumMerge(d), avgMerge(af_avg1) FROM agg_func_col GROUP BY k ORDER BY k;

SELECT '';
INSERT INTO agg_func_col (k, af_avg1) VALUES (2, arrayReduce('avgState', [101]));
SELECT k, sumMerge(d), avgMerge(af_avg1) FROM agg_func_col GROUP BY k ORDER BY k;

SELECT '';
ALTER TABLE agg_func_col ADD COLUMN af_gua AggregateFunction(groupUniqArray, String) DEFAULT arrayReduce('groupUniqArrayState', ['---', '---']);
SELECT k, sumMerge(d), avgMerge(af_avg1), groupUniqArrayMerge(af_gua) FROM agg_func_col GROUP BY k ORDER BY k;

SELECT '';
INSERT INTO agg_func_col (k, af_avg1, af_gua) VALUES (3, arrayReduce('avgState', [102, 102]), arrayReduce('groupUniqArrayState', ['igua', 'igua']));
SELECT k, sumMerge(d), avgMerge(af_avg1), groupUniqArrayMerge(af_gua) FROM agg_func_col GROUP BY k ORDER BY k;

OPTIMIZE TABLE agg_func_col;

SELECT '';
SELECT k, sumMerge(d), avgMerge(af_avg1), groupUniqArrayMerge(af_gua) FROM agg_func_col GROUP BY k ORDER BY k;

DROP TABLE IF EXISTS agg_func_col;

SELECT '';
SELECT arrayReduce('groupUniqArrayIf', ['---', '---', 't1'], [1, 1, 0]);
SELECT arrayReduce('groupUniqArrayMergeIf',
	[arrayReduce('groupUniqArrayState', ['---', '---']), arrayReduce('groupUniqArrayState', ['t1', 't'])],
	[1, 0]
);

SELECT '';
SELECT arrayReduce('avgState', [0]) IN (arrayReduce('avgState', [0, 1]), arrayReduce('avgState', [0]));
SELECT arrayReduce('avgState', [0]) IN (arrayReduce('avgState', [0, 1]), arrayReduce('avgState', [1]));

SELECT '';
SELECT arrayReduce('uniqExactMerge',
    [arrayReduce('uniqExactMergeState',
        [
            arrayReduce('uniqExactState', [12345678901]),
            arrayReduce('uniqExactState', [12345678901])
        ])
    ]);

SELECT arrayReduce('uniqExactMerge',
    [arrayReduce('uniqExactMergeState',
        [
            arrayReduce('uniqExactState', [12345678901]),
            arrayReduce('uniqExactState', [12345678902])
        ])
    ]);

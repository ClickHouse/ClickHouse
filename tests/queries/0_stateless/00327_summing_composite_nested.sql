DROP TABLE IF EXISTS summing_composite_key;
CREATE TABLE summing_composite_key (d Date, k UInt64, FirstMap Nested(k1 UInt32, k2ID Int8, s Float64), SecondMap Nested(k1ID UInt64, k2Key String, k3Type Int32, s Int64)) ENGINE = SummingMergeTree(d, k, 1);

INSERT INTO summing_composite_key VALUES ('2000-01-01', 1, [1,2], ['3','4'], [10,11], [0,1,2], [3,4,5], [-1,-2,-3], [1,10,100]), ('2000-01-01', 1, [2,1], ['4','3'], [20,22], [2,2,1], [5,5,0], [-3,-3,-33], [10,100,1000]), ('2000-01-01', 2, [1,2], ['3','4'], [10,11], [0,1,2], [3,4,5], [-1,-2,-3], [1,10,100]), ('2000-01-01', 2, [2,1,1], ['4','3','3'], [20,22,33], [2,2], [5,5], [-3,-3], [10,100]), ('2000-01-01', 2, [1,2], ['3','4'], [10,11], [0,1,2], [3,4,5], [-1,-2,-3], [1,10,100]);

SELECT * FROM summing_composite_key ORDER BY d, k, _part_index;

SELECT d, k, m.k1, m.k2ID, m.s FROM summing_composite_key ARRAY JOIN FirstMap AS m ORDER BY d, k, m.k1, m.k2ID, m.s;
SELECT d, k, m.k1, m.k2ID, sum(m.s) FROM summing_composite_key ARRAY JOIN FirstMap AS m GROUP BY d, k, m.k1, m.k2ID ORDER BY d, k, m.k1, m.k2ID;
SELECT d, k, m.k1, m.k2ID,m. s FROM summing_composite_key FINAL ARRAY JOIN FirstMap AS m ORDER BY d, k, m.k1, m.k2ID, m.s;

SELECT d, k, m.k1ID, m.k2Key, m.k3Type, m.s FROM summing_composite_key ARRAY JOIN SecondMap AS m ORDER BY d, k, m.k1ID, m.k2Key, m.k3Type, m.s;
SELECT d, k, m.k1ID, m.k2Key, m.k3Type, sum(m.s) FROM summing_composite_key ARRAY JOIN SecondMap AS m GROUP BY d, k, m.k1ID, m.k2Key, m.k3Type ORDER BY d, k, m.k1ID, m.k2Key, m.k3Type;
SELECT d, k, m.k1ID, m.k2Key, m.k3Type, m.s FROM summing_composite_key FINAL ARRAY JOIN SecondMap AS m ORDER BY d, k, m.k1ID, m.k2Key, m.k3Type, m.s;

OPTIMIZE TABLE summing_composite_key PARTITION 200001 FINAL;

SELECT * FROM summing_composite_key ORDER BY d, k, _part_index;

SELECT d, k, m.k1, m.k2ID, m.s FROM summing_composite_key ARRAY JOIN FirstMap AS m ORDER BY d, k, m.k1, m.k2ID, m.s;
SELECT d, k, m.k1, m.k2ID, sum(m.s) FROM summing_composite_key ARRAY JOIN FirstMap AS m GROUP BY d, k, m.k1, m.k2ID ORDER BY d, k, m.k1, m.k2ID;
SELECT d, k, m.k1, m.k2ID, m.s FROM summing_composite_key FINAL ARRAY JOIN FirstMap AS m ORDER BY d, k, m.k1, m.k2ID, m.s;

SELECT d, k, m.k1ID, m.k2Key, m.k3Type, m.s FROM summing_composite_key ARRAY JOIN SecondMap AS m ORDER BY d, k, m.k1ID, m.k2Key, m.k3Type, m.s;
SELECT d, k, m.k1ID, m.k2Key, m.k3Type, sum(m.s) FROM summing_composite_key ARRAY JOIN SecondMap AS m GROUP BY d, k, m.k1ID, m.k2Key, m.k3Type ORDER BY d, k, m.k1ID, m.k2Key, m.k3Type;
SELECT d, k, m.k1ID, m.k2Key, m.k3Type, m.s FROM summing_composite_key FINAL ARRAY JOIN SecondMap AS m ORDER BY d, k, m.k1ID, m.k2Key, m.k3Type, m.s;

DROP TABLE summing_composite_key;

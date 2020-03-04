Пример сравнения запросов внутри CH:

DROP TABLE res_1;
DROP TABLE res_2;
DROP TABLE diff;
CREATE TEMPORARY TABLE res_1 (key String, value1 UInt32);
CREATE TEMPORARY TABLE res_2 (key String, value2 UInt32);
CREATE TEMPORARY TABLE diff (key String, diff UInt32);
SET use_cuda_aggregation=0
INSERT INTO res_1 SELECT key, COUNT(value) FROM name_1 GROUP BY key ORDER BY key
SET use_cuda_aggregation=1
INSERT INTO res_2 SELECT key, COUNT(value) FROM name_1 GROUP BY key ORDER BY key
INSERT INTO diff SELECT key,(value1-value2) FROM res_1 ALL LEFT JOIN (SELECT key,value2 FROM res_2) USING key
SELECT SUM(diff) FROM diff
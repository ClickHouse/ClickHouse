DROP TABLE IF EXISTS tb1;

CREATE TABLE tb1 (n UInt32, a Array(Float64)) engine=Memory;
INSERT INTO tb1 VALUES (1, [-3,2.4,15,3.9,5,6,4.5,5.2,3,4,5,16,7,5,5,4]), (2, [-3,2.4,15,3.9,5,6,4.5,5.2,12,45,12,3.4,3,4,5,6]);

SELECT seriesOutliersTukey(a) FROM tb1 ORDER BY n;
DROP TABLE IF EXISTS tb1;
SELECT seriesOutliersTukey(arrayMap(x -> sin(x / 10), range(30)));
SELECT seriesOutliersTukey([-3, 2.4, 15, NULL]); -- { serverError ILLEGAL_COLUMN}
SELECT seriesOutliersTukey([]); -- { serverError ILLEGAL_COLUMN}
SELECT seriesOutliersTukey([-3, 2.4, 15]); -- { serverError BAD_ARGUMENTS}
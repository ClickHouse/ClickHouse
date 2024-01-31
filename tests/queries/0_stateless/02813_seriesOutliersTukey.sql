DROP TABLE IF EXISTS tb1;

CREATE TABLE tb1 (n UInt32, a Array(Float64)) engine=Memory;
INSERT INTO tb1 VALUES (1, [-3,2.40,15,3.90,5,6,4.50,5.20,3,4,5,16,7,5,5,4]), (2, [-3,2.40,15,3.90,5,6,4.50,5.20,12,45,12,3.40,3,4,5,6]);

-- non-const inputs
SELECT seriesOutliersDetectTukey(a) FROM tb1 ORDER BY n;
SELECT seriesOutliersDetectTukey(a,'ctukey', 25,75) FROM tb1 ORDER BY n;
DROP TABLE IF EXISTS tb1;

-- const inputs
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6]);
SELECT seriesOutliersDetectTukey([-3, 2.40, 15, 3.90, 5, 6, 4.50, 5.20, 12, 60, 12, 3.40, 3, 4, 5, 6, 3.40, 2.7]);

-- const inputs with optional arguments
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6], 'ctukey', 25, 75);
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6], 'ctukey', 10, 90);
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6], 'tukey', 10, 90);
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6], 'ctukey', 2, 98);
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3], 'ctukey', 2, 98);
SELECT seriesOutliersDetectTukey(arrayMap(x -> sin(x / 10), range(30)), 'tukey');
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4, 5, 12, 45, 12, 3, 3, 4, 5, 6], 'tukey', 25, 75, 1.5);
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4, 5, 12, 45, 12, 3, 3, 4, 5, 6], 'tukey', 25, 75, 3);

-- negative tests
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4, 5, 12, 45, 12, 3, 3, 4, 5, 6], 'tukey', 25, 75, -1); -- { serverError BAD_ARGUMENTS}
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3], 'xyz', 33, 53); -- { serverError BAD_ARGUMENTS}
SELECT seriesOutliersDetectTukey([-3, 2.4, 15, NULL]); -- { serverError ILLEGAL_COLUMN}
SELECT seriesOutliersDetectTukey([]); -- { serverError ILLEGAL_COLUMN}
SELECT seriesOutliersDetectTukey([-3, 2.4, 15]); -- { serverError BAD_ARGUMENTS}
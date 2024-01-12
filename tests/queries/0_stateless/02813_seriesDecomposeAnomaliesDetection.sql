-- Tags: no-cpu-aarch64
-- Tag no-cpu-aarch64: values generated are slighly different on aarch64

DROP TABLE IF EXISTS tb2;

CREATE TABLE tb2 (`id` UInt32, `ts` Array(Float64)) ENGINE = Memory;
INSERT INTO tb2 VALUES (1, [10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34]);
INSERT INTO tb2 VALUES (2, [139, 87, 110, 68, 54, 50, 51, 53, 133, 86, 141, 97, 156, 94, 149, 95, 140, 77, 61, 50, 54, 47, 133, 72, 152, 94, 148, 105, 162, 101, 160, 87, 63, 53, 55, 54, 151, 103, 189, 108, 183, 113, 175, 113, 178, 90, 71, 62, 62, 65, 165, 109, 181, 115, 182, 121, 178, 114, 170]);

-- non-const inputs
SELECT seriesDecomposeAnomaliesDetection(ts) FROM tb2 ORDER BY id;
SELECT seriesDecomposeAnomaliesDetection(ts, 1.5, -1, 'ctukey') FROM tb2 ORDER BY id;
DROP TABLE IF EXISTS tb2;

-- const inputs
SELECT seriesDecomposeAnomaliesDetection([139, 87, 110, 68, 54, 50, 51, 53, 133, 86, 141, 97, 156, 94, 149, 95, 140, 77, 61, 50, 54, 47, 133, 72, 152, 94, 148, 105, 162, 101, 160, 87, 63, 53, 55, 54, 151, 103, 189, 108, 183, 113, 175, 113, 178, 90, 71, 62, 62, 65, 165, 109, 181, 115, 182, 121, 178, 114, 170]);

-- const inputs with optional arguments
SELECT seriesDecomposeAnomaliesDetection([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5, -1, 'tukey');
SELECT seriesDecomposeAnomaliesDetection([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 3, 3, 'ctukey');
SELECT seriesDecomposeAnomaliesDetection([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5, 0, 'tukey');

-- negative tests
SELECT seriesDecomposeAnomaliesDetection([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, NULL]); -- { serverError ILLEGAL_COLUMN}
SELECT seriesDecomposeAnomaliesDetection([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5); --{ serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT seriesDecomposeAnomaliesDetection([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5, -1); --{ serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT seriesDecomposeAnomaliesDetection([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], -1, -1, 'tukey'); --{ serverError BAD_ARGUMENTS}
SELECT seriesDecomposeAnomaliesDetection([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5, -5, 'tukey'); --{ serverError BAD_ARGUMENTS}
SELECT seriesDecomposeAnomaliesDetection([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5, 0, 'abc'); --{ serverError BAD_ARGUMENTS}
SELECT seriesDecomposeAnomaliesDetection([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], Nan, -1, 'tukey'); --{ serverError BAD_ARGUMENTS}
SELECT seriesDecomposeAnomaliesDetection([]); -- { serverError ILLEGAL_COLUMN}
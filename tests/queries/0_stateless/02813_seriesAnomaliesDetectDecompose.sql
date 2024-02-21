-- Tags: no-cpu-aarch64, no-fasttest
-- Tag no-cpu-aarch64: values generated are slighly different on aarch64

DROP TABLE IF EXISTS tb2;

CREATE TABLE tb2 (`id` UInt32, `ts` Array(Float64)) ENGINE = Memory;
INSERT INTO tb2 VALUES (1, [10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34]);
INSERT INTO tb2 VALUES (2, [139, 87, 110, 68, 54, 50, 51, 53, 133, 86, 141, 97, 156, 94, 149, 95, 140, 77, 61, 50, 54, 47, 133, 72, 152, 94, 148, 105, 162, 101, 160, 87, 63, 53, 55, 54, 151, 103, 189, 108, 183, 113, 175, 113, 178, 90, 71, 62, 62, 65, 165, 109, 181, 115, 182, 121, 178, 114, 170]);

-- non-const inputs
SELECT seriesAnomaliesDetectDecompose(ts) FROM tb2 ORDER BY id;
SELECT seriesAnomaliesDetectDecompose(ts, 1.5, -1, 'ctukey') FROM tb2 ORDER BY id;
DROP TABLE IF EXISTS tb2;

-- const inputs
SELECT seriesAnomaliesDetectDecompose([139, 87, 110, 68, 54, 50, 51, 53, 133, 86, 141, 97, 156, 94, 149, 95, 140, 77, 61, 50, 54, 47, 133, 72, 152, 94, 148, 105, 162, 101, 160, 87, 63, 53, 55, 54, 151, 103, 189, 108, 183, 113, 175, 113, 178, 90, 71, 62, 62, 65, 165, 109, 181, 115, 182, 121, 178, 114, 170]);
SELECT seriesAnomaliesDetectDecompose(materialize([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2]));
SELECT seriesAnomaliesDetectDecompose([4, 3.0, 2.000889, 4, 3.0, 2.00076, 4, 3.0, 2.0, 4, 3.0, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2]);
SELECT seriesAnomaliesDetectDecompose(materialize([2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]));

-- const inputs with optional arguments
SELECT seriesAnomaliesDetectDecompose([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5, -1, 'tukey');
SELECT seriesAnomaliesDetectDecompose([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 3, 3, 'ctukey');
SELECT seriesAnomaliesDetectDecompose([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5, 0, 'tukey');

-- negative tests
SELECT seriesAnomaliesDetectDecompose([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, NULL]); -- { serverError ILLEGAL_COLUMN}
SELECT seriesAnomaliesDetectDecompose([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5); --{ serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT seriesAnomaliesDetectDecompose([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5, -1); --{ serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT seriesAnomaliesDetectDecompose([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], -1, -1, 'tukey'); --{ serverError BAD_ARGUMENTS}
SELECT seriesAnomaliesDetectDecompose([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5, -5, 'tukey'); --{ serverError BAD_ARGUMENTS}
SELECT seriesAnomaliesDetectDecompose([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5, 0, 'abc'); --{ serverError BAD_ARGUMENTS}
SELECT seriesAnomaliesDetectDecompose([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], Nan, -1, 'tukey'); --{ serverError BAD_ARGUMENTS}
SELECT seriesAnomaliesDetectDecompose([]); -- { serverError ILLEGAL_COLUMN}
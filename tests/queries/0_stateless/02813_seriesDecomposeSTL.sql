DROP TABLE IF EXISTS tb2;

CREATE TABLE tb2 (`period` UInt32, `ts` Array(Float64)) ENGINE = Memory;
INSERT INTO tb2 VALUES (3,[10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34]);
INSERT INTO tb2 VALUES (14, [139, 87, 110, 68, 54, 50, 51, 53, 133, 86, 141, 97, 156, 94, 149, 95, 140, 77, 61, 50, 54, 47, 133, 72, 152, 94, 148, 105, 162, 101, 160, 87, 63, 53, 55, 54, 151, 103, 189, 108, 183, 113, 175, 113, 178, 90, 71, 62, 62, 65, 165, 109, 181, 115, 182, 121, 178, 114, 170]);

SELECT seriesDecomposeSTL([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34], 3);
SELECT seriesDecomposeSTL([2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2], 0);
SELECT seriesDecomposeSTL(ts, period) FROM tb2 ORDER BY period;
DROP TABLE IF EXISTS tb2;
SELECT seriesDecomposeSTL([2,2,2,2,2,2,2,2,2,2,2,2,2,2], -5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT seriesDecomposeSTL([2,2,2,2,2,2,2,2,2,2,2,2,2,2], -5.2); --{ serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT seriesDecomposeSTL(); --{ serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT seriesDecomposeSTL([]); --{ serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT seriesDecomposeSTL([1,2,3], 2); --{ serverError BAD_ARGUMENTS}
SELECT seriesDecomposeSTL([2,2,2,3,3,3]); --{ serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT seriesDecomposeSTL([2,2,2,3,3,3], 9272653446478); --{ serverError BAD_ARGUMENTS}
SELECT seriesDecomposeSTL([2,2,2,3,3,3], 7); --{ serverError BAD_ARGUMENTS}

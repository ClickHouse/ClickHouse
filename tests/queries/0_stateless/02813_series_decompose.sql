-- Tags: no-fasttest

SELECT seriesDecomposeSTL([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34], 3);
SELECT seriesDecomposeSTL([2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2], 0);
SELECT seriesDecomposeSTL([2,2,2,2,2,2,2,2,2,2,2,2,2,2], -5); -- { serverError ILLEGAL_COLUMN}
SELECT seriesDecomposeSTL([2,2,2,2,2,2,2,2,2,2,2,2,2,2], -5.2); --{ serverError ILLEGAL_COLUMN}
SELECT seriesDecomposeSTL(); --{ serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT seriesDecomposeSTL([]); --{ serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT seriesDecomposeSTL([1,2,3], 2); --{ serverError BAD_ARGUMENTS}
SELECT seriesDecomposeSTL([2,2,2,3,3,3]); --{ serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

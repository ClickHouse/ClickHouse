SELECT('seriesWindowSum');

SELECT seriesWindowSum([1, 2, 3, 4, 5, 6], 1);
SELECT seriesWindowSum([1, 1, 1, 1, 1, 1], 2);
SELECT seriesWindowSum([4, 7, 8, 13, 24, 56, 67, 11, 12], 3);
SELECT seriesWindowSum([3, 3], 4);
SELECT seriesWindowSum([4], 1);
SELECT seriesWindowSum([1, 0, 12, 3, 4, 5, 2, 0, 1, 6], 8);

SELECT('seriesWindowAverage');

SELECT seriesWindowAverage([1, 2, 3, 4, 5, 6], 1);
SELECT seriesWindowAverage([1, 1, 1, 1, 1, 1], 2);
SELECT seriesWindowAverage([4, 7, 8, 13, 24, 56, 67, 11, 12], 3);
SELECT seriesWindowAverage([3, 3], 4);
SELECT seriesWindowAverage([4], 1);
SELECT seriesWindowAverage([1, 0, 12, 3, 4, 5, 2, 0, 1, 6], 8);

SELECT('seriesWindowMin');

SELECT seriesWindowMin([1, 2, 3, 4, 5, 6], 1);
SELECT seriesWindowMin([1, 1, 1, 1, 1, 1], 2);
SELECT seriesWindowMin([4, 7, 8, 13, 24, 56, 67, 11, 12], 3);
SELECT seriesWindowMin([3, 3], 4);
SELECT seriesWindowMin([4], 1);
SELECT seriesWindowMin([1, 0, 12, 3, 4, 5, 2, 0, 1, 6], 8);

SELECT('seriesWindowMax');

SELECT seriesWindowMax([1, 2, 3, 4, 5, 6], 1);
SELECT seriesWindowMax([1, 1, 1, 1, 1, 1], 2);
SELECT seriesWindowMax([4, 7, 8, 13, 24, 56, 67, 11, 12], 3);
SELECT seriesWindowMax([3, 3], 4);
SELECT seriesWindowMax([4], 1);
SELECT seriesWindowMax([1, 0, 12, 3, 4, 5, 2, 0, 1, 6], 8);


SELECT('seriesWindowStandardDeviation');

SELECT seriesWindowStandardDeviation([1, 2, 3, 4, 5, 6], 1);
SELECT seriesWindowStandardDeviation([1, 1, 1, 1, 1, 1], 2);
SELECT seriesWindowStandardDeviation([4, 7, 8, 13, 24, 56, 67, 11, 12], 3);
SELECT seriesWindowStandardDeviation([3, 3], 4);
SELECT seriesWindowStandardDeviation([4], 1);
SELECT seriesWindowStandardDeviation([1, 0, 12, 3, 4, 5, 2, 0, 1, 6], 8);

SELECT('seriesKPSS');

SELECT seriesKPSS([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
-- nans are ok in this context; this matches behaviour of Python statsmodels.tsa.stattools.kpss(series, regression='c', nlags=0)
-- and there are no strict garauntees for kpss to avoid nans before execution;
-- and also nans actually do provide meaningful information -- it is either short series or very stationary one
SELECT seriesKPSS([1, 1, 1, 1, 1, 1]);
SELECT seriesKPSS([4, 7, 8, 13, 24, 3, 34, 56, 67, 67, 20, 21, 56, 67, 11, 12]);
SELECT seriesKPSS([1, 0, 12, 3, 4, 5, 2, 0, 1, 6, 2, 4, 2, 0, 18, 22, 56, -18]);

SELECT('seriesEMA');

SELECT seriesEMA([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0);
SELECT seriesEMA([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0.2);
SELECT seriesEMA([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0.4);
SELECT seriesEMA([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 1);
SELECT seriesEMA([12, -12, 12, -12, 12, -12], 0.3);
SELECT seriesEMA([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1], 0.2);
SELECT seriesEMA([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1], 0.5);

SELECT('seriesKaufmansAMA');

SELECT seriesKaufmansAMA([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2);
SELECT seriesKaufmansAMA([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 4);
SELECT seriesKaufmansAMA([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3);
SELECT seriesKaufmansAMA([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1], 2);
SELECT seriesKaufmansAMA([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1], 5);


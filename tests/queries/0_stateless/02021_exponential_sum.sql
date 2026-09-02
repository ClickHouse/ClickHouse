-- Check that it is deterministic
WITH number % 10 = 0 AS value, number AS time SELECT exponentialMovingAverage(1)(value, time) AS exp_smooth FROM numbers_mt(10);
WITH number % 10 = 0 AS value, number AS time SELECT exponentialMovingAverage(1)(value, time) AS exp_smooth FROM numbers_mt(100);
WITH number % 10 = 0 AS value, number AS time SELECT exponentialMovingAverage(1)(value, time) AS exp_smooth FROM numbers_mt(1000);
WITH number % 10 = 0 AS value, number AS time SELECT exponentialMovingAverage(1)(value, time) AS exp_smooth FROM numbers_mt(10000);
WITH number % 10 = 0 AS value, number AS time SELECT exponentialMovingAverage(1)(value, time) AS exp_smooth FROM numbers_mt(100000);
WITH number % 10 = 0 AS value, number AS time SELECT exponentialMovingAverage(1)(value, time) AS exp_smooth FROM numbers_mt(1000000);
WITH number % 10 = 0 AS value, number AS time SELECT exponentialMovingAverage(1)(value, time) AS exp_smooth FROM numbers_mt(10000000);

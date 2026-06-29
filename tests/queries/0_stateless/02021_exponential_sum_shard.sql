-- Check that it is deterministic
WITH number % 10 = 0 AS value, number AS time SELECT exponentialMovingAverage(1)(value, time) AS exp_smooth FROM remote('127.0.0.{1..10}', numbers_mt(1000));
WITH number % 10 = 0 AS value, number AS time SELECT exponentialMovingAverage(1)(value, time) AS exp_smooth FROM remote('127.0.0.{1..10}', numbers_mt(10000));
WITH number % 10 = 0 AS value, number AS time SELECT exponentialMovingAverage(1)(value, time) AS exp_smooth FROM remote('127.0.0.{1..10}', numbers_mt(100000));
WITH number % 10 = 0 AS value, number AS time SELECT exponentialMovingAverage(1)(value, time) AS exp_smooth FROM remote('127.0.0.{1..10}', numbers_mt(1000000));

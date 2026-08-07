SELECT * FROM system.numbers WHERE sleepEachRow(0.05) LIMIT 10; -- { serverError TOO_SLOW }

DROP TABLE IF EXISTS 01802_empsalary;

SET allow_experimental_window_functions=1;

CREATE TABLE 01802_empsalary
(
    `depname` LowCardinality(String),
    `empno` UInt64,
    `salary` Int32,
    `enroll_date` Date
)
ENGINE = MergeTree
ORDER BY enroll_date
SETTINGS index_granularity = 8192;

INSERT INTO 01802_empsalary VALUES ('sales', 1, 5000, '2006-10-01'), ('develop', 8, 6000, '2006-10-01'), ('personnel', 2, 3900, '2006-12-23'), ('develop', 10, 5200, '2007-08-01'), ('sales', 3, 4800, '2007-08-01'), ('sales', 4, 4801, '2007-08-08'), ('develop', 11, 5200, '2007-08-15'), ('personnel', 5, 3500, '2007-12-10'), ('develop', 7, 4200, '2008-01-01'), ('develop', 9, 4500, '2008-01-01');

SELECT mannWhitneyUTest(salary, salary) OVER (ORDER BY salary ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS func FROM 01802_empsalary; -- {serverError 36}

SELECT rankCorr(salary, 0.5) OVER (ORDER BY salary ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS func FROM 01802_empsalary; -- {serverError 36}

DROP TABLE IF EXISTS 01802_empsalary;

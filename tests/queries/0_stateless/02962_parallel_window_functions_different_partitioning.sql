CREATE TABLE empsalary 
(
    `depname` LowCardinality(String),
    `empno` UInt64,
    `salary` Int32,
    `enroll_date` Date
)
ENGINE = Memory;

insert into empsalary values ('sales',3,4800,'2007-08-01'), ('sales',1,5000,'2006-10-01'), ('sales',4,4800,'2007-08-08');


insert into empsalary values ('sales',3,4800,'2007-08-01'), ('sales',1,5000,'2006-10-01'), ('sales',4,4800,'2007-08-08');

insert into empsalary values ('sales',3,4800,'2007-08-01'), ('sales',1,5000,'2006-10-01'), ('sales',4,4800,'2007-08-08');

-- 1 window function

SELECT depname,
   sum(salary) OVER (PARTITION BY depname order by empno) AS depsalary
FROM  empsalary
order by depsalary;


-- 2 window functions with different window,
-- but result should be the same for depsalary

SELECT depname,
   sum(salary) OVER (PARTITION BY depname order by empno) AS depsalary,
   min(salary) OVER (PARTITION BY depname, empno order by enroll_date) AS depminsalary
FROM  empsalary
order by depsalary;


-- The windows are planned for one thread, while the source is read with two (also under memory pressure).

SELECT depname,
   sum(salary) OVER (PARTITION BY depname order by empno) AS depsalary,
   min(salary) OVER (PARTITION BY depname, empno order by enroll_date) AS depminsalary
FROM (SELECT * FROM empsalary SETTINGS max_threads = 2, max_threads_min_free_memory_per_thread = 0)
order by depsalary
SETTINGS max_threads = 1;

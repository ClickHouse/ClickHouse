SELECT 1 FROM remote('127.0.0.{1,2}', numbers(99)) GROUP BY materialize(1) HAVING count() > 0 AND argMax(1, tuple(0))

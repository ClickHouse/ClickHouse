SET max_memory_usage = 50000000;
SET join_algorithm = 'partial_merge';

SELECT count(1) FROM (
    SELECT t2.n FROM numbers(10) t1
    JOIN (SELECT toUInt32(1) AS k, number n FROM numbers(100)) t2 ON toUInt32(t1.number) = t2.k
    JOIN (SELECT toUInt32(1) AS k, number n FROM numbers(100)) t3 ON t2.k = t3.k
    JOIN (SELECT toUInt32(1) AS k, number n FROM numbers(100)) t4 ON t2.k = t4.k
);

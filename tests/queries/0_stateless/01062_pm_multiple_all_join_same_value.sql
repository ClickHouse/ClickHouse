SET max_threads = 1; -- keep the `additional_memory_tracking_per_thread` reservation deterministic
SET max_memory_usage = 54194304; -- 50 MB + 4 MiB to absorb `additional_memory_tracking_per_thread`
SET join_algorithm = 'partial_merge';

SELECT count(1) FROM (
    SELECT t2.n FROM numbers(10) t1
    JOIN (SELECT toUInt32(1) AS k, number n FROM numbers(100)) t2 ON toUInt32(t1.number) = t2.k
    JOIN (SELECT toUInt32(1) AS k, number n FROM numbers(100)) t3 ON t2.k = t3.k
    JOIN (SELECT toUInt32(1) AS k, number n FROM numbers(100)) t4 ON t2.k = t4.k
);

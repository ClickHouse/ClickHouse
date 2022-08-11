-- Tags: no-replicated-database, no-tsan, no-asan, no-msan

SELECT least(value, 0) FROM system.metrics WHERE metric = 'MemoryTracking';
SELECT length(range(100000000));
SELECT least(value, 0) FROM system.metrics WHERE metric = 'MemoryTracking';

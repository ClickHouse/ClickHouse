--Test whether jemalloc.fragmentation metric is the difference of active and allocated

SELECT if(maxIf(value, name = 'jemalloc.active') >= maxIf(value, name = 'jemalloc.allocated'),
          maxIf(value, name = 'jemalloc.active') - maxIf(value, name = 'jemalloc.allocated'),
          0) = maxIf(value, name = 'jemalloc.fragmentation')
FROM system.asynchronous_metrics
WHERE name IN ('jemalloc.active', 'jemalloc.allocated', 'jemalloc.fragmentation')

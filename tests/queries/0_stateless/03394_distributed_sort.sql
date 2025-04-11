CREATE TABLE test(src_ip UInt32, dst_ip UInt32, bytes UInt64) ENGINE MergeTree() ORDER BY src_ip;

INSERT INTO test SELECT number%3, number%4, number FROM numbers(10);
INSERT INTO test SELECT number%5, number%3, number FROM numbers(10, 10);

SELECT dst_ip, src_ip, bytes
FROM test
WHERE bytes > 5 AND src_ip > 2
ORDER BY dst_ip, src_ip, bytes
SETTINGS make_distributed_plan=1, enable_parallel_replicas=0;

EXPLAIN SELECT dst_ip, src_ip, bytes
FROM test
WHERE bytes > 5 AND src_ip > 2
ORDER BY dst_ip, src_ip, bytes
SETTINGS make_distributed_plan=1, enable_parallel_replicas=0;


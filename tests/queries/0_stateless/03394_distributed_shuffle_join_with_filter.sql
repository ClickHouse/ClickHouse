SET optimize_exchanges = 1;
CREATE TABLE test(src_ip UInt32, dst_ip UInt32, bytes UInt64) ENGINE MergeTree() ORDER BY src_ip;

INSERT INTO test SELECT number%1000, (number+10)%1000, number%100 FROM numbers(5000);

-- t1.src_ip!=0 condition is not moved to prewhere because src_ip is in primary key

EXPLAIN
SELECT count() FROM test AS t1 JOIN test AS t2 ON t1.src_ip = t2.dst_ip WHERE t1.src_ip != 0 AND t1.bytes > 10
SETTINGS make_distributed_plan=1, enable_parallel_replicas=0;

SELECT count() FROM test AS t1 JOIN test AS t2 ON t1.src_ip = t2.dst_ip WHERE t1.src_ip != 0 AND t1.bytes > 10
SETTINGS make_distributed_plan=1, enable_parallel_replicas=0;

SELECT count() FROM test AS t1 JOIN test AS t2 ON t1.src_ip = t2.dst_ip WHERE t1.src_ip != 0 AND t1.bytes > 10
SETTINGS make_distributed_plan=0;

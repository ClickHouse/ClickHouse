set enable_parallel_replicas=1, max_parallel_replicas=2, parallel_replicas_local_plan=1;

SELECT * FROM (SELECT dummy AS k FROM remote('127.0.0.{1,2}', system.one));


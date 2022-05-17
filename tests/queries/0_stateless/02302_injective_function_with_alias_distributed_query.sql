select toString(dummy) as dummy from clusterAllReplicas('test_shard_localhost', system.one) group by dummy; 
select toString(dummy + 1) as dummy from clusterAllReplicas('test_shard_localhost', system.one) group by dummy; 

select toString(dummy)  as dummy from remote('127.0.0.{1,2}', system.one) group by dummy; 
select toString(dummy + 1)  as dummy from remote('127.0.0.{1,2}', system.one) group by dummy; 

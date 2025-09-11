-- Tags: shard

DROP TABLE IF EXISTS tmp;

CREATE OR REPLACE VIEW tmp AS SELECT initialQueryStartTime() as it, now() AS t FROM system.one WHERE NOT ignore(sleep(0.5));
SELECT now()==max(t), initialQueryStartTime()==max(it), initialQueryStartTime()==min(it), initialQueryStartTime()>=now()-1 FROM (SELECT it, t FROM remote('127.0.0.{1..10}', currentDatabase(), tmp)) SETTINGS max_distributed_connections=1, async_socket_for_remote=0;

DROP TABLE tmp;


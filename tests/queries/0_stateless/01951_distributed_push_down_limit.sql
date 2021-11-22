-- Tags: distributed

-- { echo }
explain select * from remote('127.{1,2}', view(select * from numbers(1e6))) order by number limit 10 settings distributed_push_down_limit=0;
explain select * from remote('127.{1,2}', view(select * from numbers(1e6))) order by number limit 10 settings distributed_push_down_limit=1;

-- { echo }
select shardHostName() from remote('127.1', system.one);
select shardHostName() from remote('127.2', system.one);

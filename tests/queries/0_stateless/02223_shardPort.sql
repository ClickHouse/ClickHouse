-- { echo }
select shardPort() from remote('127.1', system.one);
select shardPort() from remote('127.2', system.one);

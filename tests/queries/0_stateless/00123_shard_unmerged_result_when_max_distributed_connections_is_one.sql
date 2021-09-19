SET max_distributed_connections = 1;
SELECT count() + 1 FROM remote('127.0.0.{2,3}', system, one);

-- https://github.com/ClickHouse/ClickHouse/issues/59107
SELECT topK('102.4') FROM remote('127.0.0.{1,2}', view(SELECT NULL FROM system.one WHERE dummy = 1));

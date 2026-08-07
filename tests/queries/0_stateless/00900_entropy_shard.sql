-- Tags: shard

SELECT round(entropy(number), 6) FROM remote('127.0.0.{1,2}', numbers(256));
SELECT entropy(rand64()) > 8 FROM remote('127.0.0.{1,2}', numbers(256));

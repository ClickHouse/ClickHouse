-- Tags: shard

SELECT count() FROM remote('127.0.0.{2,3}', system, one) WHERE arrayExists((x) -> x = 1, [1, 2, 3])

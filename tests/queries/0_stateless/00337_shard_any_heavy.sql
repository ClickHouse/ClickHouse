-- Tags: shard

SELECT anyHeavy(x) FROM (SELECT intHash64(number) % 100 < 60 ? 999 : number AS x FROM system.numbers LIMIT 100000);
SELECT anyHeavy(1) FROM remote('127.0.0.{2,3}', system.one);

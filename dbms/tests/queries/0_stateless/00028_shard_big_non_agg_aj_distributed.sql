SELECT sum(s) FROM (SELECT y AS s FROM remote('127.0.0.{1,2}', test, big_array) ARRAY JOIN x AS y);
DROP TABLE test.big_array;

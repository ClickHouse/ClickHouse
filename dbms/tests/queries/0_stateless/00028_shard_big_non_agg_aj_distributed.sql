SELECT sum(s) FROM (SELECT y AS s FROM remote('127.0.0.{2,3}', default, big_array) ARRAY JOIN x AS y);

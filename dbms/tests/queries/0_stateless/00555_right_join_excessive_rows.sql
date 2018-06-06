SET max_block_size = 10;
SELECT * FROM (select toUInt64(1) s limit 1) any right join (select number s from numbers(11)) using (s) ORDER BY s;

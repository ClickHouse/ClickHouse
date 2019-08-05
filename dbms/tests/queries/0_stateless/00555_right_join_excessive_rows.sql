SET any_join_get_any_from_right_table = 1;
SET max_block_size = 10;
SELECT * FROM (select toUInt64(1) s limit 1) any right join (select number s, s as x from numbers(11)) using (s) ORDER BY s;

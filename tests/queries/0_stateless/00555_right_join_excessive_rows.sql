SET any_join_distinct_right_table_keys = 1;
SET max_block_size = 10;
SELECT * FROM (select toUInt64(1) s limit 1) js1 any right join (select number s, s as x from numbers(11)) js2 using (s) ORDER BY s;

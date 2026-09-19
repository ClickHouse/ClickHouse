-- Disable force_primary_key_reverse_order: Query plan output depends on sort direction
SET force_primary_key_reverse_order = 0;

DROP TABLE IF EXISTS test;
CREATE TABLE test(a Int, b Int) Engine=ReplacingMergeTree order by a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
set optimize_on_insert = 0;
INSERT INTO test select number, number from numbers(5);
INSERT INTO test select number, number from numbers(5,2);
set max_threads =1;
explain pipeline select * from test final SETTINGS enable_vertical_final = 0;
select * from test final;
set max_threads =2;
explain pipeline select * from test final SETTINGS enable_vertical_final = 0;
DROP TABLE test;

DROP TABLE IF EXISTS test_a;
DROP TABLE IF EXISTS test_b;
DROP TABLE IF EXISTS test_c;
DROP DATABASE IF EXISTS projection_optimization_test;

CREATE DATABASE projection_optimization_test;
USE projection_optimization_test;
SET optimize_project_query = 1;

CREATE TABLE test_a(src String, dst String, other_cols String, PROJECTION p1(SELECT src, dst ORDER BY dst)) ENGINE = MergeTree ORDER BY src;
EXPLAIN SYNTAX select * from test_a where dst = '-42';

insert into test_a select number, -number, 'other_col '||toString(number) from numbers(100);
EXPLAIN SYNTAX select * from test_a where src = '42';
EXPLAIN SYNTAX select * from test_a where dst = '-42';

CREATE TABLE test_b(src String, dst String, c1 String, c2 String, c3 String, other_cols String, PROJECTION p1(SELECT src, dst ORDER BY dst), PROJECTION p2(SELECT src, c1, c2, c3 ORDER BY c1, c2, c3)) ENGINE = MergeTree ORDER BY src;
insert into test_b select number, -number, number-1, number+1, number+2, 'other_col '||toString(number) from numbers(100);
EXPLAIN SYNTAX select * from test_b where src = '42';
EXPLAIN SYNTAX select * from test_b where c1 = '43' and c2 = '44' and c3 = '45' and dst = '-42';

EXPLAIN SYNTAX select c1 as id, c2 as id2 from test_b where lower(id) = '43' and id2 = '44';

CREATE TABLE test_c(src String, src2 String, dst String, c1 String, c2 String, c3 String, other_cols String, PROJECTION p1(SELECT src, dst ORDER BY dst), PROJECTION p2(SELECT src, c1, c2, c3 ORDER BY c1), PROJECTION p3(SELECT src, c1, c2, c3 ORDER BY c2), PROJECTION p4(SELECT src, src2 ORDER BY c3)) ENGINE = MergeTree ORDER BY (src, src2);
insert into test_c select number, number, -number, number-1, number+1, number+2, 'other_col '||toString(number) from numbers(100);
EXPLAIN SYNTAX select src2 from test_c where (src = '44' or dst = '-42') and src2 = '44';
EXPLAIN SYNTAX WITH ('-41', '-42', '-43') AS dst_list select src from test_c where dst in dst_list or src = '20';
WITH ('-41', '-42', '-43') AS dst_list select src from test_c where dst in dst_list or src = '20';

EXPLAIN SYNTAX select src from test_c where c3 = '42' and (c1 = '39' or c2 = '41');
select src from test_c where c3 = '42' and (c1 = '39' or c2 = '41');

select src from test_c where (c1 = '20' or c2 = '41');
select src from test_c where dst = '-21' and (c1 = '20' or c2 = '41');


DROP TABLE test_a;
DROP TABLE test_b;
DROP TABLE test_c;
DROP DATABASE projection_optimization_test;
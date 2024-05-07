DROP TABLE IF EXISTS test_a;
DROP TABLE IF EXISTS test_b;
SET optimize_project_query = 1;
SET allow_experimental_analyzer = 1;

CREATE TABLE test_a(src String, dst String, c1 String, c2 String, c3 String, other_cols String, PROJECTION p1(SELECT src, dst ORDER BY dst), PROJECTION p2(SELECT src, c1, c2, c3 ORDER BY c1, c2, c3)) ENGINE = MergeTree ORDER BY src;
insert into test_a select number, -number, number-1, number+1, number+2, 'other_col '||toString(number) from numbers(100);
EXPLAIN query tree select * from test_a where src = '42';
EXPLAIN query tree select * from test_a where c1 = '43' and c2 = '44' and c3 = '45' and dst = '-42';
EXPLAIN query tree select c1 as id, c2 as id2 from test_a where lower(id) = '43' and id2 = '44';
select count() from test_a where c1 != '43';
EXPLAIN query tree select count() from test_a where c1 != '43';

CREATE TABLE test_b(src String, src2 String, dst String, c1 String, c2 String, c3 String, other_cols String, PROJECTION p1(SELECT src, dst ORDER BY dst), PROJECTION p2(SELECT src, c1, c2, c3 ORDER BY c1), PROJECTION p3(SELECT src, c1, c2, c3 ORDER BY c2), PROJECTION p4(SELECT src, src2 ORDER BY c3)) ENGINE = MergeTree ORDER BY (src, src2);
insert into test_b select number, number, -number, number-1, number+1, number+2, 'other_col '||toString(number) from numbers(100);
EXPLAIN query tree select src2 from test_b where (src = '44' or dst = '-42') and src2 = '44';
EXPLAIN query tree WITH ('-41', '-42', '-43') AS dst_list select src from test_b where dst in dst_list or src = '20';
WITH ('-41', '-42', '-43') AS dst_list select src from test_b where dst in dst_list or src = '20';
WITH ('-41', '-42', '-43') AS dst_list select count() from test_b where dst not in dst_list or src = '20';
select count() from test_b where dst !='-42' and src != '24';

EXPLAIN query tree select count() from test_b where dst not in ('-41', '-42', '-43');
EXPLAIN query tree select count() from test_b where dst !='-42' and src != '24';
EXPLAIN query tree select src from test_b where c3 = '42' and (c1 = '39' or c2 = '41');
EXPLAIN query tree select src from test_b where src = '40' and (c1 = '39' or c2 = '41');

select src from test_b where c3 = '42' and (c1 = '39' or c2 = '41');
select src from test_b where (c1 = '20' or c2 = '41');
select src from test_b where dst = '-21' and (c1 = '20' or c2 = '41');

SET optimize_project_query = 0;
DROP TABLE test_a;
DROP TABLE test_b;
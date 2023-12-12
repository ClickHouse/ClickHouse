SET optimize_project_query = 1;

DROP TABLE IF EXISTS test_a;
CREATE TABLE test_a(src String, dst String, other_cols String, PROJECTION p1(SELECT src, dst ORDER BY dst)) ENGINE = MergeTree ORDER BY src;
EXPLAIN SYNTAX select * from test_a where dst = '-42';

insert into test_a select number, -number, 'other_col '||toString(number) from numbers(100);
EXPLAIN SYNTAX select * from test_a where src = '42';
EXPLAIN SYNTAX select * from test_a where dst = '-42';

DROP TABLE IF EXISTS test_b;
CREATE TABLE test_b(src String, dst String, c1 String, c2 String, c3 String, other_cols String, PROJECTION p1(SELECT src, dst ORDER BY dst), PROJECTION p2(SELECT src, c1, c2, c3 ORDER BY c1, c2, c3)) ENGINE = MergeTree ORDER BY src;
insert into test_b select number, -number, number-1, number+1, number+2, 'other_col '||toString(number) from numbers(100);
EXPLAIN SYNTAX select * from test_b where src = '42';
EXPLAIN SYNTAX select * from test_b where c1 = '43' and c2 = '44' and c3 = '45' and dst = '-42';

EXPLAIN SYNTAX select c1 as id, c2 as id2 from test_b where lower(id) = '43' and id2 = '44';

DROP TABLE test_a;
DROP TABLE test_b;
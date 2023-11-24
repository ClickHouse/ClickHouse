SET optimize_project_query = 1;

DROP TABLE IF EXISTS test_a;
CREATE TABLE test_a(src String, dst String, other_cols String, PROJECTION p1(SELECT src, dst ORDER BY dst)) ENGINE = MergeTree ORDER BY src;
EXPLAIN SYNTAX select * from test_a where src = '42';
EXPLAIN SYNTAX select * from test_a where dst = '-42';

DROP TABLE IF EXISTS test_b;
CREATE TABLE test_b(src String, dst String, c1 String, c2 String, c3 String, other_cols String, PROJECTION p1(SELECT src, dst ORDER BY dst), PROJECTION p2(SELECT src, c1, c2, c3 ORDER BY c1, c2, c3)) ENGINE = MergeTree ORDER BY src;
EXPLAIN SYNTAX select * from test_b where src = '42';
EXPLAIN SYNTAX select * from test_b where c1 = '43' and c3 = '44' and dst = '-42';
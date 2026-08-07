DROP TABLE IF EXISTS test_group_by_with_rollup_order;

CREATE TABLE test_group_by_with_rollup_order (id Int64, a Nullable(Int64), b Nullable(String)) ENGINE = MergeTree ORDER BY id;

insert into test_group_by_with_rollup_order values(1,1,'a');
insert into test_group_by_with_rollup_order values(2,2,'a');
insert into test_group_by_with_rollup_order values(3,3,'b');
insert into test_group_by_with_rollup_order values(4,4,'b');

SELECT toString(a) as r1, b, count() FROM test_group_by_with_rollup_order GROUP BY r1, b WITH ROLLUP ORDER BY b,r1;

DROP TABLE IF EXISTS test_group_by_with_rollup_order;

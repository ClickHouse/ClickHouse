drop table if exists t;
create table t (a Int8, val Float32) engine = Memory();
insert into t values (1,1.1), (1,1.2), (2,2.1);

SET enable_optimize_predicate_expression = 0;

SELECT * FROM (
    SELECT a, t1.val as val1, t2.val as val2
    FROM t t1
    ANY LEFT JOIN t t2 USING a
) ORDER BY val1;

SET enable_optimize_predicate_expression = 1;

SELECT * FROM (
    SELECT a, t1.val as val1, t2.val as val2
    FROM t t1
    ANY LEFT JOIN t t2 USING a
) ORDER BY val1;

drop table t;

drop table if exists t1;
CREATE TABLE t1 (c0 Int32) ENGINE = MergeTree() ORDER BY c0 PARTITION BY (- (c0));
insert into t1 values(1);
SELECT (- ((((tan (t1.c0)))+(t1.c0)))), (cos ((sin (pow(t1.c0,t1.c0))))), ((gcd((- (t1.c0)),((t1.c0)+(t1.c0))))*((- ((- (t1.c0)))))) FROM t1 GROUP BY (sqrt ((- (t1.c0)))), t1.c0, pow((erf ((- (t1.c0)))),t1.c0);
drop table t1;


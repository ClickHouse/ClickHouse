DROP TABLE IF EXISTS t;
create table t (i Int, a Int, s String, index ind_s (s) type set(1) granularity 1) engine = MergeTree order by i;
insert into t values (1, 1, 'a') (2, 1, 'a') (3, 1, 'a') (4, 1, 'a');
SELECT a, i from t ORDER BY a, i;
DROP TABLE t;

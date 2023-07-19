DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t0 (c0 Int16, projection h (SELECT min(c0), max(c0), count() GROUP BY -c0)) ENGINE = MergeTree ORDER BY ();

INSERT INTO t0(c0) VALUES (1);

SELECT count() FROM t0 GROUP BY gcd(-sign(c0), -c0) SETTINGS optimize_use_implicit_projections = 1;

create table t1 (c0 Int32) engine = MergeTree order by sin(c0);
insert into t1 values (-1), (1);
select c0 from t1 order by sin(-c0) settings optimize_read_in_order=0;
select c0 from t1 order by sin(-c0) settings optimize_read_in_order=1;

CREATE TABLE t2 (p Nullable(Int64), k Decimal(76, 39)) ENGINE = MergeTree PARTITION BY toDate(p) ORDER BY k SETTINGS index_granularity = 1, allow_nullable_key = 1;

INSERT INTO t2 FORMAT Values ('2020-09-01 00:01:02', 1), ('2020-09-01 20:01:03', 2), ('2020-09-02 00:01:03', 3);

SELECT count() FROM t2 WHERE indexHint(p = 1.) SETTINGS optimize_use_implicit_projections = 1;

DROP TABLE t0;
DROP TABLE t1;
DROP TABLE t2;

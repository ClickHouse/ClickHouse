DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;

CREATE TABLE t1 (id String, name String, value UInt32)
ENGINE = Join(ANY, LEFT, id)
SETTINGS join_use_nulls = 1;

CREATE TABLE t2 (id String, name String, value UInt32)
ENGINE = Join(ANY, LEFT, id)
SETTINGS join_use_nulls = 0;

CREATE TABLE t3 (id Nullable(String), name String, value UInt32)
ENGINE = Join(ANY, LEFT, id)
SETTINGS join_use_nulls = 1;

CREATE TABLE t4 (id String, name Nullable(String), value UInt32)
ENGINE = Join(ANY, LEFT, id)
SETTINGS join_use_nulls = 0;

insert into t1 values('1', 's', 1);
insert into t2 values('2', 's', 2);
insert into t3 values('3', 's', 3);
insert into t4 values('4', 's', 4);

select *, toTypeName(id), toTypeName(name) from t1;
select *, toTypeName(id), toTypeName(name) from t2;
select *, toTypeName(id), toTypeName(name) from t3;
select *, toTypeName(id), toTypeName(name) from t4;

SET join_use_nulls = 1;

select *, toTypeName(id), toTypeName(name) from t1;
select *, toTypeName(id), toTypeName(name) from t2;
select *, toTypeName(id), toTypeName(name) from t3;
select *, toTypeName(id), toTypeName(name) from t4;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;

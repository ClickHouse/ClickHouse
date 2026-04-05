set allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t1__fuzz_17 (`a` LowCardinality(UInt8), `b` Nullable(UInt256)) ENGINE = Memory;
CREATE TABLE t2__fuzz_0 (`c` UInt32, `d` String) ENGINE = Memory;

insert into t1__fuzz_17 select * from generateRandom() limit 1;
insert into t2__fuzz_0 select * from generateRandom() limit 1;

set join_algorithm='grace_hash';
SELECT * FROM t1__fuzz_17 INNER JOIN t2__fuzz_0 ON c = a WHERE a format Null;

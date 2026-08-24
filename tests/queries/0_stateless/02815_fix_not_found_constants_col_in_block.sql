DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (vkey UInt32, c0 Float32, primary key(c0)) engine = AggregatingMergeTree;
insert into t0 values (19000, 1);
select null as c_2_0, ref_2.c0 as c_2_1, ref_2.vkey as c_2_2 from t0 as ref_2 order by c_2_0 asc, c_2_1 asc, c_2_2 asc;
select null as c_2_0, ref_2.c0 as c_2_1, ref_2.vkey as c_2_2 from t0 as ref_2 order by c_2_0 asc, c_2_1 asc;
DROP TABLE t0;

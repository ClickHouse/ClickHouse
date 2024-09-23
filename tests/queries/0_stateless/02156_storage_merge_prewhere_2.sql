DROP TABLE IF EXISTS t_02156_ololo_1;
DROP TABLE IF EXISTS t_02156_ololo_2;
DROP TABLE IF EXISTS t_02156_ololo_dist;

CREATE TABLE t_02156_ololo_1 (k UInt32, v Nullable(String)) ENGINE = MergeTree order by k;
CREATE TABLE t_02156_ololo_2 (k UInt32, v String) ENGINE = MergeTree order by k;
CREATE TABLE t_02156_ololo_dist (k UInt32, v String) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_02156_ololo_2);
CREATE TABLE t_02156_ololo_dist2 (k UInt32, v Nullable(String)) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_02156_ololo_1);

insert into t_02156_ololo_1 values (1, 'a');
insert into t_02156_ololo_2 values (2, 'b');

select * from merge('t_02156_ololo') where k != 0 and notEmpty(v) order by k settings optimize_move_to_prewhere=0;
select * from merge('t_02156_ololo') where k != 0 and notEmpty(v) order by k settings optimize_move_to_prewhere=1;

select * from merge('t_02156_ololo_dist') where k != 0 and notEmpty(v) order by k settings optimize_move_to_prewhere=0;
select * from merge('t_02156_ololo_dist') where k != 0 and notEmpty(v) order by k settings optimize_move_to_prewhere=1;

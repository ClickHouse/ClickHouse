DROP TABLE IF EXISTS t;

create table t (c1 Int64, c2 String, c3 DateTime, c4 Int8, c5 String, c6 String, c7 String, c8 String, c9 String, c10 String, c11 String, c12 String, c13 Int8, c14 Int64, c15 String, c16 String, c17 String, c18 Int64, c19 Int64, c20 Int64) engine ReplicatedMergeTree('/clickhouse/test/{database}/test_02477', '1') order by c18
SETTINGS allow_remote_fs_zero_copy_replication=1;

insert into t (c1, c18) select number, -number from numbers(2000000);

alter table t add projection p_norm (select * order by c1);

optimize table t final;

alter table t materialize projection p_norm settings mutations_sync = 1;

SYSTEM FLUSH LOGS;

SELECT * FROM system.text_log WHERE event_time >= now() - 30 and level == 'Error' and message like '%BAD_DATA_PART_NAME%'and message like '%p_norm%';

DROP TABLE IF EXISTS t;

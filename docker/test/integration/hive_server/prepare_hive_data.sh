#!/bin/bash
hive -e "create database test"

hive -e "create table test.demo(id string, score int) PARTITIONED BY(day string) ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'  STORED AS INPUTFORMAT  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'; create table test.demo_orc(id string, score int) PARTITIONED BY(day string) ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'  STORED AS INPUTFORMAT  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'; "
hive -e "create table test.demo_text(id string, score int, day string)row format delimited fields terminated by ','; load data local inpath '/demo_data.txt' into table test.demo_text "
 hive -e "set hive.exec.dynamic.partition.mode=nonstrict;insert into test.demo partition(day) select * from test.demo_text; insert into test.demo_orc partition(day) select * from test.demo_text"

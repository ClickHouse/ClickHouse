#!/bin/bash
set -x

hive -e "create database if not exists test"

# Plain (non-partitioned) source table. INSERT ... VALUES is reliable here and avoids
# depending on a data file shipped inside the Hive image.
hive -e "drop table if exists test.demo_src; create table test.demo_src(id string, score int, day string); insert into test.demo_src values ('a', 1, '2021-11-01'), ('b', 2, '2021-11-05'), ('c', 3, '2021-11-05'), ('d', 4, '2021-11-11')"

# Partitioned Parquet table, populated from the source via dynamic partitioning so that
# SELECT without a WHERE clause has to collect files from several partitions.
hive -e "drop table if exists test.demo; create table test.demo(id string, score int) PARTITIONED BY(day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
hive -e "set hive.exec.dynamic.partition.mode=nonstrict; insert into test.demo partition(day) select id, score, day from test.demo_src"

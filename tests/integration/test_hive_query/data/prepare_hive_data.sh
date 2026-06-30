#!/bin/bash
# Fail (non-zero exit) if any hive command fails, so the caller can retry while the
# Hive metastore is still starting up. All statements are idempotent, so re-running
# the whole script is safe.
set -ex

# The hive CLI can hang for a long time when the metastore / HiveServer2 is not ready
# yet. Wrap every invocation in a timeout so a stuck command aborts the script (with
# `set -e`) and lets the caller retry, instead of consuming the whole test budget. Fall
# back to the previous (untimed) behaviour if `timeout` is unavailable in the image.
if command -v timeout >/dev/null 2>&1; then
    HIVE_TIMEOUT="timeout 150"
else
    HIVE_TIMEOUT=""
fi
run_hive() { $HIVE_TIMEOUT hive -e "$1"; }

run_hive "create database if not exists test"

# Plain (non-partitioned) source table. INSERT ... VALUES is reliable here and avoids
# depending on a data file shipped inside the Hive image.
run_hive "drop table if exists test.demo_src; create table test.demo_src(id string, score int, day string); insert into test.demo_src values ('a', 1, '2021-11-01'), ('b', 2, '2021-11-05'), ('c', 3, '2021-11-05'), ('d', 4, '2021-11-11')"

# Partitioned Parquet table, populated from the source via dynamic partitioning so that
# SELECT without a WHERE clause has to collect files from several partitions.
run_hive "drop table if exists test.demo; create table test.demo(id string, score int) PARTITIONED BY(day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
run_hive "set hive.exec.dynamic.partition.mode=nonstrict; insert into test.demo partition(day) select id, score, day from test.demo_src"

# Partitioned ORC table for the split-skip (stripe pruning) regression test. Two separate
# INSERTs into the same partition produce two ORC files with disjoint `score` ranges
# ([1, 2] and [10, 11]), so a filter on the non-partition `score` column prunes one file's
# stripe (a non-empty per-query split-skip set) while keeping the other. The table is
# dropped and recreated on every run, so re-running the whole script stays idempotent
# (it always ends with exactly two files).
run_hive "drop table if exists test.demo_orc; create table test.demo_orc(id string, score int) PARTITIONED BY(day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"
run_hive "insert into test.demo_orc partition(day='2021-11-01') values ('a', 1), ('b', 2)"
run_hive "insert into test.demo_orc partition(day='2021-11-01') values ('c', 10), ('d', 11)"

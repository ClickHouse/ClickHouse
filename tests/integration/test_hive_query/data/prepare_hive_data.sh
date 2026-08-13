#!/bin/bash
# Fail (non-zero exit) if any hive command fails, so the caller can retry while the
# Hive metastore is still starting up. All statements are idempotent, so re-running
# the whole script is safe.
set -ex

# The hive CLI can hang for a long time when the metastore / HiveServer2 is not ready
# yet: it blocks on the metastore connection (the previous runs were killed with exit code
# 124 on the very first statement). Wrap every invocation in a timeout so a stuck command
# aborts the script (with `set -e`) and lets the caller retry, instead of consuming the
# whole test budget. Fall back to the previous (untimed) behaviour if `timeout` is
# unavailable in the image.
if command -v timeout >/dev/null 2>&1; then
    HAVE_TIMEOUT=1
    HIVE_TIMEOUT="timeout 150"
else
    HAVE_TIMEOUT=""
    HIVE_TIMEOUT=""
fi
run_hive() { $HIVE_TIMEOUT hive -e "$1"; }

# Wait for the metastore to answer a trivial query before running the heavier statements.
# While the metastore is still starting the hive CLI blocks on the connection, so a single
# long-timeout statement wastes the whole budget on one stuck attempt. Probing with a short
# timeout instead lets many cheap connection attempts fit into the budget: a stuck probe is
# killed quickly and we poll again, while a successful one (a trivial query once the
# metastore is up) returns fast. This is best effort - if `timeout` is unavailable we skip
# straight to the statements below.
if [ -n "$HAVE_TIMEOUT" ]; then
    metastore_ready=""
    for _ in {1..12}; do
        if timeout 30 hive -e "show databases" >/dev/null 2>&1; then
            metastore_ready=1
            break
        fi
        sleep 2
    done
    # If the metastore still isn't answering, abort (via `set -e`) so the caller retries from
    # scratch rather than blocking on the long-timeout statements below.
    if [ -z "$metastore_ready" ]; then
        echo "Hive metastore is not ready yet, aborting for retry" >&2
        exit 1
    fi
fi

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

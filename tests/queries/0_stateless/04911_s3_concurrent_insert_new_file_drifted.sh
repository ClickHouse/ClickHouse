#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

filename="test_04911_${CLICKHOUSE_DATABASE}_${RANDOM}"
writers=8

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04911"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04911 (a UInt64) ENGINE = S3(s3_conn, filename='$filename', format=Parquet)"

# A numbered object must exist remotely while being absent from the in-memory path list, because the
# next name is derived from that list's length. Dropping an S3 table deletes no objects and a fresh
# table starts from a single-element list, so the two inserts below survive the drop as an object the
# new table does not know about.
# Exactly one numbered object, which is the tightest window: the first name every writer derives is
# already taken, so each one probes at least once more while holding no claim on what it skipped.
$CLICKHOUSE_CLIENT --s3_truncate_on_insert 1 -q "INSERT INTO t_04911 SELECT 0"
$CLICKHOUSE_CLIENT --s3_create_new_file_on_insert 1 -q "INSERT INTO t_04911 SELECT 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04911"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04911 (a UInt64) ENGINE = S3(s3_conn, filename='$filename', format=Parquet)"

writer_pids=()
for i in $(seq 100 $((writers + 99))); do
    $CLICKHOUSE_CLIENT --s3_create_new_file_on_insert 1 -q "INSERT INTO t_04911 SELECT $i" &
    writer_pids+=("$!")
done
wait "${writer_pids[@]}"

# Scoped to the writers: the two pre-drop rows are excluded so that this asserts name allocation
# only, independently of which pre-existing objects the table lists.
# `countDistinct(a)`, not `count()`: two writers deriving the same name both open it with
# `WriteMode::Rewrite`, and that name is then listed twice, so the losing writer's row is missing
# while the winner's is counted twice and the total row count still matches.
$CLICKHOUSE_CLIENT -q "SELECT countDistinct(a) = $writers, countDistinct(_file) = $writers FROM t_04911 WHERE a >= 100"

# The path list is also the set of objects `TRUNCATE` removes, so it must never name an object this
# table did not write. The numbered object created before the drop is exactly such an object: it is
# present remotely and unknown to this table, so it has to survive a truncate.
# `s3_ignore_file_doesnt_exist` keeps a deleted object as an empty read rather than an error, so the
# assertion below reports the outcome instead of aborting the script.
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_04911"
$CLICKHOUSE_CLIENT --s3_ignore_file_doesnt_exist 1 \
    -q "SELECT count() = 1 FROM s3(s3_conn, filename='$filename.1', format=Parquet, structure='a UInt64')"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04911"

#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

filename="test_04910_${CLICKHOUSE_DATABASE}_${RANDOM}"
writers=8

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04910"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04910 (a UInt64) ENGINE = S3(s3_conn, filename='$filename', format=Parquet)"

# The next file name is derived from the path list, so the base object must exist for a new one
# to be derived at all.
$CLICKHOUSE_CLIENT --s3_truncate_on_insert 1 -q "INSERT INTO t_04910 SELECT 0"

for i in $(seq 1 $writers); do
    if (( i % 2 == 0 )); then
        $CLICKHOUSE_CLIENT --s3_create_new_file_on_insert 1 --async_insert 1 --wait_for_async_insert 1 \
            -q "INSERT INTO t_04910 SELECT $i" &
    else
        $CLICKHOUSE_CLIENT --s3_create_new_file_on_insert 1 \
            -q "INSERT INTO t_04910 SELECT $i" &
    fi
done
wait

# Every writer must have landed in a file of its own: a name derived twice means one writer
# overwrote another.
$CLICKHOUSE_CLIENT -q "SELECT count() = $writers + 1, countDistinct(_file) = $writers + 1 FROM t_04910"
$CLICKHOUSE_CLIENT -q "SELECT groupArray(a) = range(toUInt64($writers + 1)) FROM (SELECT a FROM t_04910 ORDER BY a)"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04910"

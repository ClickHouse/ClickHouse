#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

declare -a engines=("Log" "TinyLog" "StripeLog")
for engine in "${engines[@]}"
do
    echo "$engine:"

    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS tbl"
    $CLICKHOUSE_CLIENT --query="CREATE TABLE tbl(x UInt32, y String) ENGINE=$engine"
    data_dir=$($CLICKHOUSE_CLIENT --query="SELECT data_paths[1] FROM system.tables WHERE name='tbl' AND database=currentDatabase()")

    echo "empty:"
    find "$data_dir"* 2>/dev/null

    echo "1 element:"
    $CLICKHOUSE_CLIENT --query="INSERT INTO tbl VALUES (1, 'a')"
    $CLICKHOUSE_CLIENT --query="SELECT * FROM tbl ORDER BY x"
    for name in $(find "$data_dir"* -print0 | xargs -0 -n 1 basename | sort); do
        file_path=$data_dir$name
        file_size=$(stat -c%s "$file_path")
        echo "$name size=$file_size"
        hexdump -C $file_path
    done

    echo "3 elements:"
    $CLICKHOUSE_CLIENT --query="INSERT INTO tbl VALUES (22, 'bc'), (333, 'def')"
    $CLICKHOUSE_CLIENT --query="SELECT * FROM tbl ORDER BY x"
    for name in $(find "$data_dir"* -print0 | xargs -0 -n 1 basename | sort); do
        file_path=$data_dir$name
        file_size=$(stat -c%s "$file_path")
        echo "$name size=$file_size"
        hexdump -C $file_path
    done

    echo
done

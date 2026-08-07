#!/usr/bin/env bash

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
    declare -A file_sizes
    for name in $(find "$data_dir"* -print0 | xargs -0 -n 1 basename | sort); do
        file_path=$data_dir$name
        file_size=$(stat -c%s "$file_path")
        file_sizes[$name]=$file_size
        echo $name
    done

    echo "3 elements:"
    $CLICKHOUSE_CLIENT --query="INSERT INTO tbl VALUES (22, 'bc'), (333, 'def')"
    $CLICKHOUSE_CLIENT --query="SELECT * FROM tbl ORDER BY x"
    for name in $(find "$data_dir"* -print0 | xargs -0 -n 1 basename | sort); do
        file_path=$data_dir$name
        file_size=$(stat -c%s "$file_path")
        old_file_size=${file_sizes[$name]}
        if [ "$name" == "sizes.json" ]; then
            cmp=""
        elif (( file_size > old_file_size )); then
            cmp="greater size"
        else
            cmp="unexpected size ($file_size, old_size=$old_file_size)"
        fi
        echo $name $cmp
    done

    echo
done

#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Dumps the files of the table.
#
# `Log` and `TinyLog` store each column with its own codec. The columns are pinned to `LZ4`, which keeps the
# dumps independent of the server default codec, and these files are dumped as they are.
#
# `StripeLog` instead writes one shared `data.bin` (and its `index.mrk`) with the server default codec regardless
# of the column codec (currently `ZSTD(3)`). The bytes and the sizes of its compressed blocks depend on the
# version of the compression library, so `data.bin` is dumped decompressed, and only the decompressed size is
# printed for `index.mrk`, which holds the offsets of the compressed blocks. A size in `sizes.json` is replaced
# with `OK` only if it is the size of the file. What the index says about the data is checked by `reattach_table`.
dump_files()
{
    for name in $(find "$data_dir"* -print0 | xargs -0 -n 1 basename | sort); do
        file_path=$data_dir$name
        if [ "$engine" != "StripeLog" ]; then
            file_size=$(stat -c%s "$file_path")
            echo "$name size=$file_size"
            hexdump -C "$file_path"
        elif [ "$name" == "data.bin" ]; then
            echo "$name decompressed:"
            ${CLICKHOUSE_COMPRESSOR} --decompress < "$file_path" | hexdump -C
        elif [ "$name" == "index.mrk" ]; then
            echo "$name decompressed size=$(${CLICKHOUSE_COMPRESSOR} --decompress < "$file_path" | wc -c)"
        else
            data_size=$(stat -c%s "${data_dir}data.bin")
            index_size=$(stat -c%s "${data_dir}index.mrk")
            echo "$name: $(sed \
                -e "s/\"data%2Ebin\":{\"size\":\"$data_size\"}/\"data%2Ebin\":{\"size\":\"OK\"}/" \
                -e "s/\"index%2Emrk\":{\"size\":\"$index_size\"}/\"index%2Emrk\":{\"size\":\"OK\"}/" \
                "$file_path")"
        fi
    done
}

# Makes the next query read the table from its files: while the table is attached, the index of `StripeLog`
# and the marks of `Log` are served from memory, so a wrong index or wrong sizes on disk would stay unnoticed.
reattach_table()
{
    $CLICKHOUSE_CLIENT --query="DETACH TABLE tbl"
    $CLICKHOUSE_CLIENT --query="ATTACH TABLE tbl"
}

declare -a engines=("Log" "TinyLog" "StripeLog")
for engine in "${engines[@]}"
do
    echo "$engine:"

    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS tbl"
    # Pin the column codec to `LZ4`, see `dump_files`.
    $CLICKHOUSE_CLIENT --query="CREATE TABLE tbl(x UInt32 CODEC(LZ4), y String CODEC(LZ4)) ENGINE=$engine"
    data_dir=$($CLICKHOUSE_CLIENT --query="SELECT data_paths[1] FROM system.tables WHERE name='tbl' AND database=currentDatabase()")

    echo "empty:"
    find "$data_dir"* 2>/dev/null

    echo "1 element:"
    $CLICKHOUSE_CLIENT --query="INSERT INTO tbl VALUES (1, 'a')"
    reattach_table
    $CLICKHOUSE_CLIENT --query="SELECT * FROM tbl ORDER BY x"
    dump_files

    echo "3 elements:"
    $CLICKHOUSE_CLIENT --query="INSERT INTO tbl VALUES (22, 'bc'), (333, 'def')"
    reattach_table
    $CLICKHOUSE_CLIENT --query="SELECT * FROM tbl ORDER BY x"
    dump_files

    echo
done

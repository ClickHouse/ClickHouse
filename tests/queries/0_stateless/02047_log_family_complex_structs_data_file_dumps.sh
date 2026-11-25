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
# version of the compression library, so `data.bin` is dumped decompressed, only the decompressed size is
# printed for `index.mrk` (it holds the offsets of the compressed blocks), and the sizes are masked in `sizes.json`.
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
            echo "$name: $(sed -E 's/"size":"[0-9]+"/"size":"N"/g' "$file_path")"
        fi
    done
}

declare -a engines=("Log" "TinyLog" "StripeLog")
for engine in "${engines[@]}"
do
    echo "$engine:"

    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS tbl"
    # Pin the column codec to `LZ4`, see `dump_files`.
    $CLICKHOUSE_CLIENT --query="CREATE TABLE tbl(x Array(Array(Int32)) CODEC(LZ4), y Nested(z String, w Float32) CODEC(LZ4)) ENGINE=$engine"
    data_dir=$($CLICKHOUSE_CLIENT --query="SELECT data_paths[1] FROM system.tables WHERE name='tbl' AND database=currentDatabase()")

    echo "empty:"
    find "$data_dir"* 2>/dev/null

    echo "1 element:"
    $CLICKHOUSE_CLIENT --query="INSERT INTO tbl VALUES ([[3,5,7]], ['a1', 'a2'], [4.1, 4.2])"
    $CLICKHOUSE_CLIENT --query="SELECT * FROM tbl ORDER BY x"
    dump_files

    echo "3 elements:"
    $CLICKHOUSE_CLIENT --query="INSERT INTO tbl VALUES ([], [], []), ([[10]], ['a3'], [4.3])"
    $CLICKHOUSE_CLIENT --query="SELECT * FROM tbl ORDER BY x"
    dump_files

    echo
done

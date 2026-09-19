#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# compress_per_substream_in_compact_parts cuts a new compressed block at a column substream boundary, but
# only once the block reached min_compress_block_size, so small substreams keep sharing a block. Measures
# the decompressed size through query_log ProfileEvents, which also works on object storage.

# $1 table, $2 columns, $3 compress_per_substream_in_compact_parts, $4 min_compress_block_size, $5 row expression
create_and_fill()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $1"
    $CLICKHOUSE_CLIENT -q "
    CREATE TABLE $1 ($2) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
        index_granularity = 8192, index_granularity_bytes = 1073741824,
        min_compress_block_size = $4, max_compress_block_size = 1048576,
        write_marks_for_substreams_in_compact_parts = 1, compress_per_column_in_compact_parts = 1,
        compress_per_substream_in_compact_parts = $3"
    $CLICKHOUSE_CLIENT -q "INSERT INTO $1 SELECT $5 FROM numbers(16384)"
}

# Uncompressed bytes the read had to decompress. $1 table, $2 read expression, $3 tag
read_bytes()
{
    local comment="${CLICKHOUSE_DATABASE}_$1_$3"
    $CLICKHOUSE_CLIENT -q "SELECT $2 FROM $1 FORMAT Null SETTINGS log_comment = '$comment'"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['CompressedReadBufferBytes'] FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '$comment' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1"
}

part_size()
{
    $CLICKHOUSE_CLIENT -q "
    SELECT sum(bytes_on_disk) FROM system.parts
    WHERE database = currentDatabase() AND table = '$1' AND active"
}

# A subcolumn read must not decompress the column's large substreams.
BIG_COLUMNS="t Tuple(big String, small String), arr Array(String)"
BIG_ROW="(hex(randomString(250)), toString(number % 100)), arrayMap(i -> hex(randomString(50)), range(5))"
create_and_fill big_on "$BIG_COLUMNS" 1 65536 "$BIG_ROW"
create_and_fill big_off "$BIG_COLUMNS" 0 65536 "$BIG_ROW"

for read_expr in "sum(length(t.small))" "sum(arr.size0)"; do
    on=$(read_bytes big_on "$read_expr" "on_${read_expr}")
    off=$(read_bytes big_off "$read_expr" "off_${read_expr}")
    [ "$off" -ge $((on * 5)) ] && echo "selective_read $read_expr OK" \
        || echo "selective_read $read_expr FAIL (on=$on off=$off)"
done

# Many small substreams must keep sharing a block: cutting at every boundary (min_compress_block_size = 1)
# makes the same read touch a much smaller block and costs storage.
small_columns=""
small_row=""
for i in $(seq 0 19); do
    small_columns="$small_columns${small_columns:+, }c$i UInt8"
    small_row="$small_row${small_row:+, }toUInt8(number + $i)"
done

create_and_fill small_adaptive "t Tuple($small_columns)" 1 65536 "tuple($small_row)"
create_and_fill small_every "t Tuple($small_columns)" 1 1 "tuple($small_row)"
create_and_fill small_off "t Tuple($small_columns)" 0 65536 "tuple($small_row)"

adaptive=$(read_bytes small_adaptive "sum(t.c0)" adaptive)
every=$(read_bytes small_every "sum(t.c0)" every)
[ "$adaptive" -ge $((every * 4)) ] && echo "adaptive_block OK" || echo "adaptive_block FAIL (adaptive=$adaptive every=$every)"

size_adaptive=$(part_size small_adaptive)
size_every=$(part_size small_every)
size_off=$(part_size small_off)
[ "$size_adaptive" -le $((size_off * 3 / 2)) ] && echo "size_near_packed OK" \
    || echo "size_near_packed FAIL (adaptive=$size_adaptive off=$size_off)"
[ "$size_every" -ge $((size_adaptive * 2)) ] && echo "every_boundary_costs_size OK" \
    || echo "every_boundary_costs_size FAIL (every=$size_every adaptive=$size_adaptive)"

# With the adaptive layout a substream mark can point into the middle of a compressed block.
$CLICKHOUSE_CLIENT -q "
SELECT if((SELECT sum(cityHash64(t.c0, t.c7, t.c19)) FROM small_adaptive)
        = (SELECT sum(cityHash64(t.c0, t.c7, t.c19)) FROM small_off), 'subcolumn_values OK', 'subcolumn_values FAIL')"

$CLICKHOUSE_CLIENT -q "DROP TABLE big_on, big_off, small_adaptive, small_every, small_off"

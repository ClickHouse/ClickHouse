#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: an escape line (a line whose first character is `%`) must be skipped even
# when the `%` falls exactly on a `ReadBuffer` refill boundary. The line-start detection must
# not peek at the byte before the current position, because at a refill boundary that byte is
# no longer in the buffer. Tiny read buffers force a refill boundary before every byte.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for buffer_size in 1 3 16
do
    echo "Buffer size: $buffer_size"
    $CLICKHOUSE_LOCAL -q "
        SELECT event, result, moves
        FROM file('$CUR_DIR/data_pgn/escape_lines.pgn', PGN, 'event String, result String, moves String')
        SETTINGS storage_file_read_method = 'pread', max_read_buffer_size = $buffer_size"
done

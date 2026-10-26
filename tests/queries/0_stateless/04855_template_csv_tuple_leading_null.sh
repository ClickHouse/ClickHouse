#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ROW_FORMAT="$CLICKHOUSE_TMP/04855_template_row_$CLICKHOUSE_DATABASE.tmp"
echo -ne '${t:CSV},${b:CSV}\n' > "$ROW_FORMAT"

for DATA in '2,1,7' '\N,1,7'; do
    echo -ne "$DATA\n" | $CLICKHOUSE_LOCAL --structure "t Tuple(Nullable(Int32), Int32), b UInt8" \
        --input-format Template --format_template_row "$ROW_FORMAT" \
        --format_template_rows_between_delimiter '' --input_format_null_as_default 1 \
        -q 'SELECT * FROM table'
done

rm "$ROW_FORMAT"

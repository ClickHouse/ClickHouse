#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the PNG format requires libpng and base64, and the fast-test build does not enable base64

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Wrapper column types (LowCardinality, Const, Sparse) must produce the same image as plain columns,
# because they are materialized before rendering. We check this by comparing the PNG bytes.
png_md5()
{
    $CLICKHOUSE_CLIENT --output_format_image_width="$1" --output_format_image_height="$2" --query "$3 FORMAT PNG" | md5sum | cut -d' ' -f1
}

# LowCardinality channels vs plain numeric channels.
plain=$(png_md5 4 2 "SELECT toUInt8(number * 60) AS r, toUInt8(number * 30) AS g, toUInt8(255 - number * 50) AS b FROM numbers(8)")
lc=$(png_md5 4 2 "SELECT toLowCardinality(toUInt8(number * 60)) AS r, toLowCardinality(toUInt8(number * 30)) AS g, toLowCardinality(toUInt8(255 - number * 50)) AS b FROM numbers(8)")
[[ "$plain" == "$lc" ]] && echo "LowCardinality RGB: identical" || echo "LowCardinality RGB: DIFFERENT"

# Const channels (constant folding) vs materialized plain channels.
plain=$(png_md5 2 2 "SELECT materialize(toUInt8(10)) AS r, materialize(toUInt8(20)) AS g, materialize(toUInt8(30)) AS b FROM numbers(4)")
cnst=$(png_md5 2 2 "SELECT toUInt8(10) AS r, toUInt8(20) AS g, toUInt8(30) AS b FROM numbers(4)")
[[ "$plain" == "$cnst" ]] && echo "Const RGB: identical" || echo "Const RGB: DIFFERENT"

# LowCardinality(Nullable) grayscale (null is rendered as 0) vs plain Nullable.
plain=$(png_md5 2 2 "SELECT if(number = 1, NULL, toUInt8(number * 50)) AS v FROM numbers(4)")
lcn=$(png_md5 2 2 "SELECT toLowCardinality(if(number = 1, NULL, toUInt8(number * 50))) AS v FROM numbers(4)")
[[ "$plain" == "$lcn" ]] && echo "LowCardinality(Nullable) grayscale: identical" || echo "LowCardinality(Nullable) grayscale: DIFFERENT"

# Implicit coordinate mode: more rows than the image can hold is an error (no silent truncation).
$CLICKHOUSE_CLIENT --output_format_image_width=2 --output_format_image_height=1 --query "
    SELECT toUInt8(number) AS v FROM numbers(3) FORMAT PNG
" 2>&1 1>/dev/null | grep -oE "TOO_MANY_ROWS" | head -1

# Fewer rows than the image holds is fine (remaining pixels stay background).
$CLICKHOUSE_CLIENT --output_format_image_width=4 --output_format_image_height=4 --query "
    SELECT toUInt8(number) AS v FROM numbers(3) FORMAT PNG
" > /dev/null && echo "underfull image: ok"

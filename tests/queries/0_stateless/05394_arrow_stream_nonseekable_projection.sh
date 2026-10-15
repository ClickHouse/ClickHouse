#!/usr/bin/env bash
# Tags: no-fasttest
# `ArrowStream` is not available in fast-test builds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.XXXXXX")
trap 'rm -rf "$data_dir"' EXIT

# Unselected columns exceed the input buffer, so a seek cannot be satisfied
# by moving within already buffered bytes. Use multiple record batches to
# check that skipping also leaves the stream positioned at the next message.
${CLICKHOUSE_LOCAL} --query "
    SELECT number AS x, repeat('a', 262144) AS padding,
           number + 1 AS y, repeat('b', 262144) AS tail
    FROM numbers(4)
    SETTINGS max_block_size = 2, output_format_arrow_compression_method = 'none',
             output_format_arrow_record_batch_size = 0
    FORMAT ArrowStream
" > "$data_dir/input.arrowstream"

for query in 'SELECT sum(x), sum(y) FROM data' 'SELECT sum(x) FROM data'; do
    # Regular files retain the seek optimization.
    ${CLICKHOUSE_LOCAL} --table data --structure 'x UInt64, padding String, y UInt64, tail String' \
        --input-format ArrowStream --file "$data_dir/input.arrowstream" --query "$query"

    # A real pipe uses a file-descriptor buffer too, but cannot seek.
    cat "$data_dir/input.arrowstream" | ${CLICKHOUSE_LOCAL} \
        --table data --structure 'x UInt64, padding String, y UInt64, tail String' \
        --input-format ArrowStream --query "$query"
done

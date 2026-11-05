#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When the query result is not written to the terminal (e.g. stdout is redirected to a pipe),
# the progress bar must not be cleared before writing every block of data, otherwise it flickers.
# The progress bar is redrawn after every block, and it must be cleared only at the end of the query.

${CLICKHOUSE_LOCAL} --progress=err --max_block_size 1000 --query "SELECT number FROM numbers(1000000)" 2>&1 >/dev/null \
    | LC_ALL=C awk '
        { progress += gsub(/ Progress: /, ""); clears += gsub(/\r\033\[K/, ""); }
        END { print (progress >= 100 ? "progress rendered" : "progress not rendered: " progress); print (clears <= 5 ? "no flicker" : "flicker: " clears); }'

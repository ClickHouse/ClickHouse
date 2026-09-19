#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXTERNAL_DIR="$CUR_DIR/${CLICKHOUSE_TEST_UNIQUE_NAME}_scalar_file_target"
LINK_PATH="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_scalar_file_link"

mkdir -p "$EXTERNAL_DIR"
echo -n 'legacy scalar contents' > "$EXTERNAL_DIR/data.txt"
ln -s "$EXTERNAL_DIR" "$LINK_PATH"

function cleanup()
{
    rm -f "$LINK_PATH"
    rm -rf "$EXTERNAL_DIR"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "SELECT file('${CLICKHOUSE_TEST_UNIQUE_NAME}_scalar_file_link/data.txt')"

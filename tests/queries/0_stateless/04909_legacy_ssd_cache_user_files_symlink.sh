#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXTERNAL_DIR="$CUR_DIR/${CLICKHOUSE_TEST_UNIQUE_NAME}_ssd_cache_target"
LINK_PATH="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_ssd_cache_link"

mkdir -p "$EXTERNAL_DIR"
ln -s "$EXTERNAL_DIR" "$LINK_PATH"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY IF EXISTS legacy_ssd_cache_user_files_symlink" > /dev/null
    rm -f "$LINK_PATH"
    rm -rf "$EXTERNAL_DIR"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE DICTIONARY legacy_ssd_cache_user_files_symlink
    (
        id UInt64,
        value String DEFAULT ''
    )
    PRIMARY KEY id
    SOURCE(NULL())
    LIFETIME(0)
    LAYOUT(SSD_CACHE(SIZE_IN_CELLS 1024 PATH '${LINK_PATH}/cache'))"

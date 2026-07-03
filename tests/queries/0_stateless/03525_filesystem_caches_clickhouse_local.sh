#!/usr/bin/env bash
# Until recently, clickhouse-local supported only caches from the disk configuration,
# but not the standalone filesystem_caches configuration.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG_FILE="${CLICKHOUSE_TMP}/config.yaml"

> "${CONFIG_FILE}" echo "
filesystem_caches:
    cache:
        path: '${CLICKHOUSE_TMP}/cache/'
        max_size: '1G'
"

$CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" --filesystem_cache_name cache --query "
SELECT cache_name, extract(path, '/cache/$') AS path, max_size FROM system.filesystem_cache_settings FORMAT Vertical
"

rm "${CONFIG_FILE}"

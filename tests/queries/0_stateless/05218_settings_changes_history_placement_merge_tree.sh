#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `compatibility` pin gives the server-wide `MergeTree` settings the defaults of the pinned release:
# `enable_row_mask_update_coalescing` is enabled by default since 26.8.
for compatibility in 26.8 26.7
do
    ${CLICKHOUSE_LOCAL} --compatibility "${compatibility}" -q "
        SELECT '${compatibility}', value FROM system.merge_tree_settings WHERE name = 'enable_row_mask_update_coalescing'"
done

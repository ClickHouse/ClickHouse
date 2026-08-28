#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings
# A full-definition `ATTACH TABLE t UUID '...' (...) ENGINE = ...` is CREATE-like user input, so it
# must pass the same engine validation as `CREATE TABLE`. The explicit UUID has to be unique per
# run (the mapping is server-global), hence a shell test with generated UUIDs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

new_uuid() { ${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()"; }

echo 'the experimental gate applies to a full-definition ATTACH'
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE mtq_gated UUID '$(new_uuid)' (a UInt64) ENGINE = MergeTreeQueue" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'

echo 'PARTITION BY is rejected in a full-definition ATTACH'
${CLICKHOUSE_CLIENT} --allow_experimental_merge_tree_queue=1 --query "ATTACH TABLE mtq_partitioned UUID '$(new_uuid)' (p UInt64, a UInt64) ENGINE = MergeTreeQueue PARTITION BY p" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

echo 'a reader-only virtual column in the sorting key is rejected in a full-definition ATTACH'
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE mt_reader_virtual UUID '$(new_uuid)' (x UInt8) ENGINE = MergeTree ORDER BY _part" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

echo 'a sorting key over disabled block columns is rejected in a full-definition ATTACH'
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE mt_block_virtual UUID '$(new_uuid)' (x UInt8) ENGINE = MergeTree ORDER BY (_block_number, _block_offset)" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

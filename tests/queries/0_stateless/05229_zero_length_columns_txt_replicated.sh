#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-object-storage, no-replicated-database

# A `columns.txt` left at length zero by a power loss is handled like an absent one, and the column
# list is rebuilt from the table metadata. `ReplicatedMergeTree` loads its parts with
# `require_part_metadata = true`, where a rebuilt list is only accepted when `columns_substreams.txt`
# recorded the part's own column list and the rebuilt list matches it exactly. Without that record
# there is nothing to verify the rebuilt list against, so the part is still detached as broken.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS rmt SYNC;
    CREATE TABLE rmt (id UInt64, val UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05229_rmt', 'r1')
        ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
    INSERT INTO rmt SELECT number, number + 1 FROM numbers(500);
    SELECT 'before', count(), sum(val) FROM rmt;
"

PART_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'rmt' AND active")

truncate -s 0 "${PART_PATH}/columns.txt"

${CLICKHOUSE_CLIENT} -q "
    DETACH TABLE rmt;
    ATTACH TABLE rmt;
    SELECT 'recovered', count(), sum(val) FROM rmt;
    SELECT 'detached parts', count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 'rmt';
"

# Without `columns_substreams.txt` the rebuilt list cannot be verified, so the part stays broken
# and the replica will re-fetch it from a healthy peer instead of guessing.
truncate -s 0 "${PART_PATH}/columns.txt"
rm -f "${PART_PATH}/columns_substreams.txt"

# Loading the part fails here, and the server-side log message would land on the client's stderr.
${CLICKHOUSE_CLIENT} --send_logs_level=none -q "
    DETACH TABLE rmt;
    ATTACH TABLE rmt;
    SELECT 'unverifiable', count(), sum(val) FROM rmt;
    SELECT 'detached parts', count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 'rmt';
" 2>/dev/null

${CLICKHOUSE_CLIENT} -q "DROP TABLE rmt SYNC"

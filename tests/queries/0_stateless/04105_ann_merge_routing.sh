#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database

# Q-T18 canary: merge three parts into one and verify the ANN routing path still finds both
# query vectors' seeded rows. The merge redistributes `_block_number` — the partition_hash +
# block_number + block_offset triplet stored in the index must still identify the rows inside
# the single merged part.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_ann_merge"

wait_for_full_coverage() {
    local deadline=$((SECONDS + 120))
    while [ $SECONDS -lt $deadline ]; do
        local row
        row=$($CLICKHOUSE_CLIENT -q "SELECT tupleElement(tableANNCoverage(currentDatabase(), '$TABLE'), 'total') = tupleElement(tableANNCoverage(currentDatabase(), '$TABLE'), 'covered')")
        if [ "$row" = "1" ]; then
            return 0
        fi
        sleep 0.2
    done
    echo "TIMEOUT waiting for ANN coverage on $TABLE" >&2
    return 1
}

$CLICKHOUSE_CLIENT -m <<EOSQL
DROP TABLE IF EXISTS $TABLE;

CREATE TABLE $TABLE
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    ann_group_min_rows = 1,
    ann_group_max_rows = 100000,
    ann_group_max_parts = 256;

-- Three inserts => three parts. Each insert contains a planted "target" vector we'll search for.
INSERT INTO $TABLE VALUES (1, [1.0, 0.0, 0.0, 0.0]);
INSERT INTO $TABLE VALUES (2, arrayMap(i -> toFloat32(i), range(4)));
INSERT INTO $TABLE VALUES (3, [0.0, 0.0, 0.0, 1.0]);
INSERT INTO $TABLE VALUES (4, arrayMap(i -> toFloat32(i + 1), range(4)));
INSERT INTO $TABLE VALUES (5, [0.5, 0.5, 0.5, 0.5]);
INSERT INTO $TABLE VALUES (6, arrayMap(i -> toFloat32(-i), range(4)));

OPTIMIZE TABLE $TABLE FINAL;

-- After OPTIMIZE FINAL: 6 rows across 1 part; distinct _block_number values remain visible.
SELECT uniq(_block_number), count() FROM $TABLE;

-- Build ANN index on top of the merged part.
SYSTEM BUILD ANN INDEX $TABLE;
EOSQL

wait_for_full_coverage

$CLICKHOUSE_CLIENT -m <<'EOSQL'
-- Query with the planted vector for row 1; expect id = 1 returned.
SELECT id FROM t_ann_merge
ORDER BY L2Distance(emb, [1.0, 0.0, 0.0, 0.0]::Array(Float32))
LIMIT 1;

-- Query with the planted vector for row 3; expect id = 3 returned.
SELECT id FROM t_ann_merge
ORDER BY L2Distance(emb, [0.0, 0.0, 0.0, 1.0]::Array(Float32))
LIMIT 1;

DROP TABLE t_ann_merge;
EOSQL

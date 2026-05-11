#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database

# EXPLAIN the ANN query plan and verify the second pass substitutes `_distance` for the
# distance function. The rewrite is unconditional — it fires both with rescoring on and off,
# and even when unindexed parts are present.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_ann_explain"

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
    ann_group_min_rows = 1;

INSERT INTO $TABLE
SELECT number, arrayMap(i -> toFloat32((number + i) % 7), range(4))
FROM numbers(100);

SYSTEM BUILD ANN INDEX $TABLE;
EOSQL

wait_for_full_coverage

$CLICKHOUSE_CLIENT -m <<'EOSQL'
-- Base case: _distance must appear, distance function must be gone.
SELECT countIf(position(line, '_distance') > 0) > 0, countIf(position(line, 'FUNCTION L2Distance') > 0)
FROM
(
    SELECT explain AS line FROM
    (
        EXPLAIN PLAN actions = 1
        SELECT id FROM t_ann_explain
        ORDER BY L2Distance(emb, [0.0, 1.0, 2.0, 3.0]::Array(Float32))
        LIMIT 5
    )
);

-- Same outcome with vector_search_with_rescoring = 1 (this is an `ann` index, not vector_similarity).
SELECT countIf(position(line, '_distance') > 0) > 0, countIf(position(line, 'FUNCTION L2Distance') > 0)
FROM
(
    SELECT explain AS line FROM
    (
        EXPLAIN PLAN actions = 1
        SELECT id FROM t_ann_explain
        ORDER BY L2Distance(emb, [0.0, 1.0, 2.0, 3.0]::Array(Float32))
        LIMIT 5
        SETTINGS vector_search_with_rescoring = 1
    )
);
EOSQL

$CLICKHOUSE_CLIENT -q "INSERT INTO $TABLE SELECT number + 1000, arrayMap(i -> toFloat32((number + i) % 5), range(4)) FROM numbers(50)"

# Mixed scenario: unindexed parts present — the rewrite is unconditional, we don't wait for build.
$CLICKHOUSE_CLIENT -m <<'EOSQL'
SELECT countIf(position(line, '_distance') > 0) > 0, countIf(position(line, 'FUNCTION L2Distance') > 0)
FROM
(
    SELECT explain AS line FROM
    (
        EXPLAIN PLAN actions = 1
        SELECT id FROM t_ann_explain
        ORDER BY L2Distance(emb, [0.0, 1.0, 2.0, 3.0]::Array(Float32))
        LIMIT 5
    )
);

-- Negative: the SELECT list references `emb` directly, so the second pass bails out early and
-- leaves the plan untouched. The plan must NOT contain `_distance` and MUST still contain a
-- `FUNCTION L2Distance` node.
SELECT countIf(position(line, '_distance') > 0) = 0, countIf(position(line, 'FUNCTION L2Distance') > 0) > 0
FROM
(
    SELECT explain AS line FROM
    (
        EXPLAIN PLAN actions = 1
        SELECT id, emb FROM t_ann_explain
        ORDER BY L2Distance(emb, [0.0, 1.0, 2.0, 3.0]::Array(Float32))
        LIMIT 5
    )
);

DROP TABLE t_ann_explain;
EOSQL

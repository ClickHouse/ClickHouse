#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database

# End-to-end ANN (DiskANN) query path: Recall, PREWHERE coexistence, mixed indexed/unindexed,
# and `vector_search_with_rescoring` independence.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_ann_query"

# Block until every active part of $TABLE is covered by an active ANN group, or fail the test
# on timeout. `SYSTEM BUILD ANN INDEX` is fire-and-forget: it dispatches one build round to
# the dedicated BG executor and returns. We poll `tableANNCoverage` to observe completion.
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
    $CLICKHOUSE_CLIENT -q "SELECT tableANNCoverage(currentDatabase(), '$TABLE')" >&2 || true
    return 1
}

$CLICKHOUSE_CLIENT -m <<EOSQL
DROP TABLE IF EXISTS $TABLE;

CREATE TABLE $TABLE
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 16) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    ann_group_min_rows = 1,
    ann_group_max_rows = 100000,
    ann_group_max_parts = 256;

-- Deterministic dataset: 1000 rows x 16 dim, values driven by rand64 with fixed seed per row.
INSERT INTO $TABLE
SELECT
    number,
    arrayMap(i -> toFloat32(rand64(number * 16 + i) % 1000) / 1000.0, range(16))
FROM numbers(1000);

SYSTEM BUILD ANN INDEX $TABLE;
EOSQL

wait_for_full_coverage

$CLICKHOUSE_CLIENT -m <<'EOSQL'
-- Recall@10: top-10 results from the ANN path must coincide (at least 9/10) with the exact top-10.
SELECT
    count() >= 9
FROM
(
    SELECT id FROM t_ann_query
    ORDER BY L2Distance(emb, (SELECT arrayMap(i -> toFloat32(i) / 16.0, range(16))))
    LIMIT 10
) AS ann
WHERE id IN (
    SELECT id FROM t_ann_query
    ORDER BY L2Distance(emb, (SELECT arrayMap(i -> toFloat32(i) / 16.0, range(16))))
    LIMIT 10
    SETTINGS try_use_ann_search = 0
);

-- PREWHERE + ORDER BY distance LIMIT k: must return exactly k rows matching PREWHERE.
SELECT count()
FROM
(
    SELECT id FROM t_ann_query
    WHERE id < 500
    ORDER BY L2Distance(emb, (SELECT arrayMap(i -> toFloat32(i) / 16.0, range(16))))
    LIMIT 5
);
EOSQL

$CLICKHOUSE_CLIENT -q "INSERT INTO $TABLE SELECT number + 10000, arrayMap(i -> toFloat32(rand64(number * 16 + i + 1) % 1000) / 1000.0, range(16)) FROM numbers(200)"

wait_for_full_coverage

$CLICKHOUSE_CLIENT -m <<'EOSQL'
-- Mixed scenario: build caught up with the second insert; Recall@10 still holds.
SELECT
    count() >= 9
FROM
(
    SELECT id FROM t_ann_query
    ORDER BY L2Distance(emb, (SELECT arrayMap(i -> toFloat32(i) / 16.0, range(16))))
    LIMIT 10
) AS ann
WHERE id IN (
    SELECT id FROM t_ann_query
    ORDER BY L2Distance(emb, (SELECT arrayMap(i -> toFloat32(i) / 16.0, range(16))))
    LIMIT 10
    SETTINGS try_use_ann_search = 0
);

-- `vector_search_with_rescoring` must not affect the `ann` index path.
SELECT
    (
        SELECT groupArray(id) FROM
        (
            SELECT id FROM t_ann_query
            ORDER BY L2Distance(emb, (SELECT arrayMap(i -> toFloat32(i) / 16.0, range(16))))
            LIMIT 10
            SETTINGS vector_search_with_rescoring = 0
        )
    )
    =
    (
        SELECT groupArray(id) FROM
        (
            SELECT id FROM t_ann_query
            ORDER BY L2Distance(emb, (SELECT arrayMap(i -> toFloat32(i) / 16.0, range(16))))
            LIMIT 10
            SETTINGS vector_search_with_rescoring = 1
        )
    );
EOSQL

# --- Real mixed scenario: disable background build so the next insert stays unindexed at query
# time. This exercises `MergeTreeRangeReader::fillDistanceColumnFromEmbRuntime` on the unindexed
# part while the previous parts continue to serve hits via the indexed lookup path. The earlier
# "Mixed scenario" block always waited for full coverage, so this is the only section that
# actually hits the runtime distance path in a SQL test.
$CLICKHOUSE_CLIENT -q "ALTER TABLE $TABLE MODIFY SETTING ann_enable = 0"
$CLICKHOUSE_CLIENT -q "INSERT INTO $TABLE SELECT number + 20000, arrayMap(i -> toFloat32(rand64(number * 16 + i + 2) % 1000) / 1000.0, range(16)) FROM numbers(300)"

$CLICKHOUSE_CLIENT -m <<'EOSQL'
-- Confirm the table is in a genuinely mixed state (indexed parts + at least one unindexed part).
SELECT
    tupleElement(tableANNCoverage(currentDatabase(), 't_ann_query'), 'total')
  > tupleElement(tableANNCoverage(currentDatabase(), 't_ann_query'), 'covered');

-- Recall@10 in the mixed regime. Indexed parts look up pre-computed distances; the unindexed
-- part computes distances at runtime. The optimizer's second pass is unconditional, so both
-- branches feed into the same `_distance` sort key.
SELECT count() >= 9 FROM (
    SELECT id FROM t_ann_query
    ORDER BY L2Distance(emb, (SELECT arrayMap(i -> toFloat32(i) / 16.0, range(16))))
    LIMIT 10
) AS ann
WHERE id IN (
    SELECT id FROM t_ann_query
    ORDER BY L2Distance(emb, (SELECT arrayMap(i -> toFloat32(i) / 16.0, range(16))))
    LIMIT 10
    SETTINGS try_use_ann_search = 0
);

EOSQL

# ProfileEvents smoke check: a query that the optimizer routed through the index must bump
# `DiskANNSearchCount`. Use a unique query_id so we can pin the lookup against system.query_log
# regardless of the parallel test runner. Only check non-zero — actual counts depend on the
# number of groups searched and would be flaky to assert on.
PROFILE_QUERY_ID="04103_ann_profileevents_$$_$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$PROFILE_QUERY_ID" -q "
    SELECT id FROM t_ann_query
    ORDER BY L2Distance(emb, (SELECT arrayMap(i -> toFloat32(i) / 16.0, range(16))))
    LIMIT 10
    FORMAT Null"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['DiskANNSearchCount'] > 0
    FROM system.query_log
    WHERE query_id = '$PROFILE_QUERY_ID' AND type = 'QueryFinish'
    LIMIT 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_ann_query"

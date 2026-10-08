#!/usr/bin/env bash

# Tests that training of an IVF vector similarity index fails if the clustering function returns centroids with a dimension
# other than the declared one: no centroids are stored, assignCentroid rejects inserts and queries run without the rewrite.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The samples have 3 dimensions, the index declares 2.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE samples (vec Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO samples SELECT [toFloat32(number), 0, 1] FROM numbers(100);
CREATE TABLE tab (id UInt64, cid UInt32, vec Array(Float32),
    INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4)))
ENGINE = MergeTree ORDER BY (cid, id);
" --allow_experimental_vector_ivf_index 1

# Wait for the training to fail.
for _ in {1..600}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE event_date >= yesterday() AND type = 'ExceptionWhileProcessing'
        AND has(databases, currentDatabase()) AND exception LIKE '%The clustering function must return centroids with 2 dimensions%'") -gt 0 ]] && break
    sleep 0.2
done
echo "training failed"

$CLICKHOUSE_CLIENT -q "SELECT assignCentroid([toFloat32(1), 0], 'tab')" 2>&1 | grep -o "produced no centroids" | head -1
$CLICKHOUSE_CLIENT -q "INSERT INTO tab VALUES (1, 0, [1., 0.]), (2, 0, [5., 0.])"
$CLICKHOUSE_CLIENT -q "SELECT groupArray(id) FROM (SELECT id FROM tab ORDER BY L2Distance(vec, [4., 0.]) LIMIT 2)"
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT id FROM tab ORDER BY L2Distance(vec, [4., 0.]) LIMIT 2" | grep -c "cid in" || true

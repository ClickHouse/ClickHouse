#!/usr/bin/env bash

# Tests when queries on a table with an IVF vector similarity index are not rewritten to the nearest clusters:
# when the query already filters by the cluster id column, and when the reference vector is not a constant.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

POINT="arrayMap(j -> toFloat32((cityHash64(number % 8, j) % 2001) / 100 - 10 + ((cityHash64(number, j) % 1001) / 1000 - 0.5)), range(4))"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE samples (vec Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO samples SELECT $POINT FROM numbers(2000);
CREATE TABLE tab (id UInt64, vec Array(Float32), cid UInt32 DEFAULT assignCentroid(vec, 'tab'),
    INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 4, samples = 'samples', clustering = hierarchicalKMeans(8, 2, 20, 2000, 1)))
ENGINE = MergeTree ORDER BY (cid, id);
" --allow_experimental_vector_ivf_index 1

for _ in {1..600}; do
    $CLICKHOUSE_CLIENT -q "SELECT assignCentroid([toFloat32(0), 0, 0, 0], 'tab')" > /dev/null 2>&1 && break
    sleep 0.2
done
$CLICKHOUSE_CLIENT -q "INSERT INTO tab (id, vec) SELECT number, $POINT FROM numbers(8000)"

QUERY="SELECT id FROM tab ORDER BY L2Distance(vec, [1., 2., 3., 4.]) LIMIT 5"
FAR_CID=$($CLICKHOUSE_CLIENT -q "SELECT cid FROM tab GROUP BY cid ORDER BY L2Distance(avgForEach(vec), [1., 2., 3., 4.]) DESC LIMIT 1")

echo "Constant reference vector: rewritten"
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 $QUERY SETTINGS ivf_nprobe = 2" | grep -o "cid in [0-9]*-element set"

echo "Filter on the cluster id column: not rewritten, and the rows of the farthest cluster are found"
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT id FROM tab WHERE cid = $FAR_CID ORDER BY L2Distance(vec, [1., 2., 3., 4.]) LIMIT 5 SETTINGS ivf_nprobe = 2" | grep -c "element set" || true
$CLICKHOUSE_CLIENT -q "SELECT count() FROM (SELECT id FROM tab WHERE cid = $FAR_CID ORDER BY L2Distance(vec, [1., 2., 3., 4.]) LIMIT 5) SETTINGS ivf_nprobe = 2"

echo "Non-constant reference vector: not rewritten"
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT id FROM tab ORDER BY L2Distance(vec, arrayMap(x -> toFloat32(randCanonical()), range(4))) LIMIT 5 SETTINGS ivf_nprobe = 2" | grep -c "element set" || true

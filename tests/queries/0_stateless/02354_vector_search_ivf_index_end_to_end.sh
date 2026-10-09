#!/usr/bin/env bash

# End-to-end test of the IVF vector similarity index: centroids are trained in the background from a sample table, rows are
# assigned to clusters by the DEFAULT expression, and vector search reads only the nearest clusters with high recall.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# 256 well-separated cluster centres in 16 dimensions; every row is a centre plus a small noise.
POINT="arrayMap(j -> toFloat32((cityHash64(c, j) % 2001) / 100 - 10 + ((cityHash64(n, j) % 1001) / 1000 - 0.5)), range(16))"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE samples (vec Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO samples SELECT $POINT FROM (SELECT number + 1000000 AS n, n % 256 AS c FROM numbers(50000));

CREATE TABLE tab
(
    id UInt64,
    vec Array(Float32),
    cid UInt64 DEFAULT assignCentroid(vec, '${CLICKHOUSE_DATABASE}.tab'),
    INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 16, samples = 'samples', clustering = hierarchicalKMeans(256, 16, 20, 50000, 42))
)
ENGINE = MergeTree ORDER BY (cid, id)
SETTINGS index_granularity = 1024
" --allow_experimental_vector_ivf_index 1

# Training runs in the background after CREATE TABLE returns; until it is complete, the centroids are not available.
for _ in {1..600}; do
    $CLICKHOUSE_CLIENT -q "SELECT assignCentroid(arrayWithConstant(16, toFloat32(0)), 'tab')" > /dev/null 2>&1 && break
    sleep 0.2
done

echo "Insert"
$CLICKHOUSE_CLIENT -q "INSERT INTO tab (id, vec) SELECT n, $POINT FROM (SELECT number AS n, n % 256 AS c FROM numbers(200000)) SETTINGS max_insert_threads = 4"
$CLICKHOUSE_CLIENT -q "SELECT count(), uniqExact(cid) <= 256 FROM tab"

# The query vector of row ID, computed from the same formula as the data, so that the query does not read the table.
function query_vector() { local p=${POINT//c,/($1 % 256),}; echo "${p//n,/$1,}"; }
function search() { echo "SELECT groupArray(id) FROM (SELECT id FROM tab ORDER BY L2Distance(vec, $(query_vector "$1")), id LIMIT 10)"; }

echo "Only the probed clusters are read"
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 $(search 123) SETTINGS ivf_nprobe = 4" | grep -o "cid in [0-9]*-element set"
$CLICKHOUSE_CLIENT -q "$(search 123) SETTINGS ivf_nprobe = 4, max_rows_to_read = 20000, read_overflow_mode = 'throw'" > /dev/null && echo "read less than 20000 rows"

echo "ivf_epsilon probes only the clusters close to the nearest one"
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 $(search 123) SETTINGS ivf_nprobe = 256, ivf_epsilon = 0.5" | grep -o "cid in [0-9]*-element set"

echo "Recall@10 of 10 queries with ivf_nprobe = 4 compared to the exact search"
found=0
for i in {0..9}; do
    id=$((i * 19997 + 11))
    exact=$($CLICKHOUSE_CLIENT -q "$(search $id) SETTINGS ivf_nprobe = 0")
    approximate=$($CLICKHOUSE_CLIENT -q "$(search $id) SETTINGS ivf_nprobe = 4")
    found=$((found + $($CLICKHOUSE_CLIENT -q "SELECT length(arrayIntersect($exact, $approximate))")))
done
echo "recall >= 0.9: $((found >= 90))"

$CLICKHOUSE_CLIENT -q "DROP TABLE tab; DROP TABLE samples;"

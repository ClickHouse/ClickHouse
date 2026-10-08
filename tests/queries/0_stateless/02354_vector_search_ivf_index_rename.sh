#!/usr/bin/env bash

# Tests that tables with an IVF vector similarity index can be renamed and exchanged within a database, and keep using their
# own centroids for the cluster ids assigned by the DEFAULT expression. Moving them to another database is rejected.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

POINT="arrayMap(j -> toFloat32((cityHash64(number % 32, j) % 2001) / 100 - 10 + ((cityHash64(number, j) % 1001) / 1000 - 0.5)), range(4))"

# t8 and t16 have 8 and 16 clusters; their DEFAULT expressions refer to the tables by qualified and unqualified name.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE samples (vec Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO samples SELECT $POINT FROM numbers(5000);
CREATE TABLE t8 (id UInt64, vec Array(Float32), cid UInt32 DEFAULT assignCentroid(vec, '${CLICKHOUSE_DATABASE}.t8'),
    INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 4, samples = 'samples', clustering = hierarchicalKMeans(8, 2, 20, 5000, 1)))
ENGINE = MergeTree ORDER BY (cid, id);
CREATE TABLE t16 (id UInt64, vec Array(Float32), cid UInt32 DEFAULT assignCentroid(vec, 't16'),
    INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 4, samples = 'samples', clustering = hierarchicalKMeans(16, 2, 20, 5000, 1)))
ENGINE = MergeTree ORDER BY (cid, id);
" --allow_experimental_vector_ivf_index 1

# Wait until the centroids of both tables are trained.
for table in t8 t16; do
    for _ in {1..600}; do
        $CLICKHOUSE_CLIENT -q "SELECT assignCentroid([toFloat32(0), 0, 0, 0], '$table')" > /dev/null 2>&1 && break
        sleep 0.2
    done
done

function insert_and_check()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO $1 (id, vec) SELECT number, $POINT FROM numbers(20000)"
    $CLICKHOUSE_CLIENT -q "SELECT '$1', max(cid) $2 FROM $1"
}

echo "EXCHANGE TABLES: each table keeps its own centroids"
$CLICKHOUSE_CLIENT -q "EXCHANGE TABLES t8 AND t16"
insert_and_check t8 "> 8"
insert_and_check t16 "<= 8"

echo "RENAME TABLE"
$CLICKHOUSE_CLIENT -q "RENAME TABLE t8 TO t_renamed"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_renamed"
insert_and_check t_renamed "> 8"
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT id FROM t_renamed ORDER BY L2Distance(vec, [1., 2., 3., 4.]) LIMIT 5 SETTINGS ivf_nprobe = 2" | grep -o "cid in [0-9]*-element set"

echo "Moving to another database is rejected"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_other"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_other.plain (id UInt64, vec Array(Float32), cid UInt32) ENGINE = MergeTree ORDER BY (cid, id)"
$CLICKHOUSE_CLIENT -q "RENAME TABLE t16 TO ${CLICKHOUSE_DATABASE}_other.t16" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
$CLICKHOUSE_CLIENT -q "EXCHANGE TABLES t16 AND ${CLICKHOUSE_DATABASE}_other.plain" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLICKHOUSE_DATABASE}_other"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_renamed; DROP TABLE t16; DROP TABLE samples;"

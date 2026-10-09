#!/usr/bin/env bash

# Tests the contract for the result of the clustering function of an IVF vector similarity index: a tree of centroids as named
# tuples is accepted (the index assigns its own cluster ids to the leaves, in their order), malformed trees fail training, and
# other result types are rejected.
# groupArray over prepared centroids stands in for a clustering function which returns a tree.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TREE="Tuple(cid UInt32, vec Array(Float32), parent UInt32, level UInt8, radius Float32)"

function create_table()
{
    $CLICKHOUSE_CLIENT --allow_experimental_vector_ivf_index 1 -q "
    CREATE TABLE $1 (id UInt64, vec Array(Float32), cid UInt64 DEFAULT assignCentroid(vec, '$1'),
        INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = '$2', clustering = groupArray(4)))
    ENGINE = MergeTree ORDER BY (cid, id)"
}

# The elements of the tuple are matched by name; the leaves keep their order.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE samples_tree (vec $TREE) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO samples_tree VALUES ((10, [0, 0], 0, 0, 1)), ((20, [10, 0], 0, 0, 1)), ((30, [0, 10], 0, 0, 1)), ((40, [10, 10], 0, 0, 1));
CREATE TABLE samples_duplicate_cid (vec $TREE) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO samples_duplicate_cid VALUES ((10, [0, 0], 0, 0, 1)), ((10, [10, 0], 0, 0, 1)), ((30, [0, 10], 0, 0, 1)), ((40, [10, 10], 0, 0, 1));
CREATE TABLE samples_top_parent (vec $TREE) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO samples_top_parent VALUES ((10, [0, 0], 7, 0, 1)), ((20, [10, 0], 7, 0, 1)), ((30, [0, 10], 7, 0, 1)), ((40, [10, 10], 7, 0, 1));
CREATE TABLE samples_unnamed (vec Tuple(Array(Float32), UInt8, UInt32, UInt32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO samples_unnamed SELECT ([0, 0], 0, number + 1, 0) FROM numbers(4);
CREATE TABLE samples_no_parent (vec Tuple(vec Array(Float32), level UInt8, cid UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO samples_no_parent SELECT ([0, 0], 0, number + 1) FROM numbers(4);
"

create_table tab samples_tree
create_table tab_duplicate_cid samples_duplicate_cid
create_table tab_top_parent samples_top_parent

echo "Tree of centroids"
for _ in {1..600}; do
    $CLICKHOUSE_CLIENT -q "SELECT assignCentroid([toFloat32(0), 0], 'tab')" > /dev/null 2>&1 && break
    sleep 0.2
done
$CLICKHOUSE_CLIENT -q "INSERT INTO tab (id, vec) VALUES (1, [1, 1]), (2, [9, 1]), (3, [1, 9]), (4, [9, 9])"
$CLICKHOUSE_CLIENT -q "SELECT id, dense_rank() OVER (ORDER BY cid) FROM tab ORDER BY id"

echo "Malformed trees fail training"
for _ in {1..600}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE event_date >= yesterday() AND type = 'ExceptionWhileProcessing'
        AND has(databases, currentDatabase()) AND exception LIKE '%The clustering function must return%'") -ge 2 ]] && break
    sleep 0.2
done
$CLICKHOUSE_CLIENT -q "SELECT DISTINCT extract(exception, 'The clustering function must return [^:]*') AS message FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'ExceptionWhileProcessing' AND has(databases, currentDatabase())
    AND exception LIKE '%The clustering function must return%' ORDER BY message"

echo "Other result types are rejected"
create_table tab_unnamed samples_unnamed 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
create_table tab_no_parent samples_no_parent 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

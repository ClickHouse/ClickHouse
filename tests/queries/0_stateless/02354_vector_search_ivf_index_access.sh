#!/usr/bin/env bash

# Tests the grants needed for a table with an IVF vector similarity index: its hidden objects need none. Searching requires
# SELECT on the table, assignCentroid with the table name requires SELECT or INSERT on it, and CREATE TABLE requires SELECT
# on the samples table. Training reads the samples with the row policies of the user who created the table.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_RW="${CLICKHOUSE_DATABASE}_rw"
USER_NONE="${CLICKHOUSE_DATABASE}_none"
USER_CREATE="${CLICKHOUSE_DATABASE}_create"
DB="${CLICKHOUSE_DATABASE}"
INDEX="INDEX idx (vec, cid) TYPE vector_similarity('ivf', 'L2Distance', 2, samples = 'samples', clustering = hierarchicalKMeans(4))"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE samples (vec Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO samples SELECT [toFloat32(number % 10), toFloat32(number % 7)] FROM numbers(1000);
CREATE TABLE tab (id UInt64, vec Array(Float32), cid UInt64 DEFAULT assignCentroid(vec, 'tab'), $INDEX)
ENGINE = MergeTree ORDER BY (cid, id);
DROP USER IF EXISTS $USER_RW, $USER_NONE, $USER_CREATE;
CREATE USER $USER_RW IDENTIFIED WITH no_password;
CREATE USER $USER_NONE IDENTIFIED WITH no_password;
CREATE USER $USER_CREATE IDENTIFIED WITH no_password;
GRANT SELECT, INSERT ON $DB.tab TO $USER_RW;
GRANT CREATE TABLE ON $DB.* TO $USER_CREATE;
GRANT TABLE ENGINE ON MergeTree TO $USER_CREATE;
" --allow_experimental_vector_ivf_index 1

for _ in {1..600}; do
    $CLICKHOUSE_CLIENT -q "SELECT assignCentroid([toFloat32(0), 0], 'tab')" > /dev/null 2>&1 && break
    sleep 0.2
done

echo "SELECT and INSERT on the table are enough"
$CLICKHOUSE_CLIENT --user "$USER_RW" -q "INSERT INTO $DB.tab (id, vec) SELECT number, [toFloat32(number % 10), toFloat32(number % 7)] FROM numbers(100)"
$CLICKHOUSE_CLIENT --user "$USER_RW" -q "SELECT count() FROM (SELECT id FROM $DB.tab ORDER BY L2Distance(vec, [1., 1.]) LIMIT 3 SETTINGS ivf_nprobe = 2)"
$CLICKHOUSE_CLIENT --user "$USER_RW" -q "EXPLAIN indexes = 1 SELECT id FROM $DB.tab ORDER BY L2Distance(vec, [1., 1.]) LIMIT 3 SETTINGS ivf_nprobe = 2" | grep -o "cid in [0-9]*-element set"

echo "Without grants on the table"
$CLICKHOUSE_CLIENT --user "$USER_NONE" -q "SELECT assignCentroid([toFloat32(0), 0], '$DB.tab')" 2>&1 | grep -o "ACCESS_DENIED" | head -1

echo "CREATE TABLE requires SELECT on the samples table"
$CLICKHOUSE_CLIENT --user "$USER_CREATE" --allow_experimental_vector_ivf_index 1 -q "
CREATE TABLE $DB.tab2 (id UInt64, vec Array(Float32), cid UInt64, $INDEX) ENGINE = MergeTree ORDER BY (cid, id)" 2>&1 | grep -o "ACCESS_DENIED" | head -1
$CLICKHOUSE_CLIENT -q "EXISTS TABLE tab2"

echo "Training reads the samples with the row policies of the user who created the table"
$CLICKHOUSE_CLIENT -q "
GRANT SELECT ON $DB.samples TO $USER_CREATE;
CREATE ROW POLICY ${DB}_hide_samples ON $DB.samples USING 0 TO $USER_CREATE;"
$CLICKHOUSE_CLIENT --user "$USER_CREATE" --allow_experimental_vector_ivf_index 1 -q "
CREATE TABLE $DB.tab3 (id UInt64, vec Array(Float32), cid UInt64, $INDEX) ENGINE = MergeTree ORDER BY (cid, id)"
for _ in {1..600}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE event_date >= yesterday() AND type = 'ExceptionWhileProcessing'
        AND has(databases, currentDatabase()) AND exception LIKE '%The clustering function must return 4 centroids at level 0%'") -gt 0 ]] && break
    sleep 0.2
done
$CLICKHOUSE_CLIENT -q "SELECT assignCentroid([toFloat32(0), 0], 'tab3')" 2>&1 | grep -o "produced no centroids" | head -1
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY ${DB}_hide_samples ON $DB.samples"

$CLICKHOUSE_CLIENT -q "DROP USER $USER_RW, $USER_NONE, $USER_CREATE"

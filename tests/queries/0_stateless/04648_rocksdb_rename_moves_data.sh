#!/usr/bin/env bash
# Tags: use-rocksdb, no-fasttest, no-parallel, no-ordinary-database
# no-ordinary-database: the test needs its own database to be Atomic for the Atomic<->Ordinary directions.
# no-parallel: named databases plus directories under the server data path.

# Creation of a database with Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# optimize_trivial_approximate_count_query is pinned even though 0 is its product default: the
# stateless runner randomizes it to 1 in half of all runs, and with 1 a SELECT count() is answered
# from RocksDB's estimated key count instead of reading the table, so the count assertions below
# would stop proving that the moved directory is the one being read.
CLIENT="$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary 1 --optimize_trivial_approximate_count_query 0"

ORD1="${CLICKHOUSE_DATABASE}_ord1"
ORD2="${CLICKHOUSE_DATABASE}_ord2"
EXPLICIT_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"

$CLIENT -q --multiline "
    DROP DATABASE IF EXISTS $ORD1;
    DROP DATABASE IF EXISTS $ORD2;
    CREATE DATABASE $ORD1 ENGINE = Ordinary;
    CREATE DATABASE $ORD2 ENGINE = Ordinary;
"

# DETACH ... SYNC + ATTACH recreates the storage object from metadata, which is the same path
# a server restart takes: the data directory is recomputed from the table's current location.
function reloaded_count()
{
    local db=$1 table=$2
    $CLIENT -q --multiline "
        DETACH TABLE $db.$table SYNC;
        ATTACH TABLE $db.$table;
        SELECT count() FROM $db.$table;
    "
}

function make_table()
{
    local db=$1 table=$2 rows=$3 engine=$4
    $CLIENT -q --multiline "
        DROP TABLE IF EXISTS $db.$table SYNC;
        CREATE TABLE $db.$table (k UInt64, v String) ENGINE = $engine PRIMARY KEY k;
        INSERT INTO $db.$table SELECT number, toString(number) FROM numbers($rows);
    "
}

# 1. Atomic -> Ordinary. This is the case reported in the issue.
make_table "$CLICKHOUSE_DATABASE" t1 60 "EmbeddedRocksDB"
$CLIENT -q "RENAME TABLE $CLICKHOUSE_DATABASE.t1 TO $ORD1.t1"
echo "1 atomic->ordinary live $($CLIENT -q "SELECT count() FROM $ORD1.t1")"
echo "1 atomic->ordinary reloaded $(reloaded_count "$ORD1" t1)"

# 2. Ordinary -> Ordinary, different database.
make_table "$ORD1" t2 11 "EmbeddedRocksDB"
$CLIENT -q "RENAME TABLE $ORD1.t2 TO $ORD2.t2"
echo "2 ordinary->ordinary live $($CLIENT -q "SELECT count() FROM $ORD2.t2")"
echo "2 ordinary->ordinary reloaded $(reloaded_count "$ORD2" t2)"

# 3. Rename inside a single Ordinary database. No cross-database move at all, yet the computed
# data path changes, so this loses data too.
make_table "$ORD1" t3 10 "EmbeddedRocksDB"
$CLIENT -q "RENAME TABLE $ORD1.t3 TO $ORD1.t3_renamed"
echo "3 ordinary same-db live $($CLIENT -q "SELECT count() FROM $ORD1.t3_renamed")"
echo "3 ordinary same-db reloaded $(reloaded_count "$ORD1" t3_renamed)"

# 4. Ordinary -> Atomic. A fresh UUID is assigned, so before the fix the recomputed directory
# does not exist at all and the table cannot be attached back.
make_table "$ORD1" t4 13 "EmbeddedRocksDB"
$CLIENT -q "RENAME TABLE $ORD1.t4 TO $CLICKHOUSE_DATABASE.t4"
echo "4 ordinary->atomic live $($CLIENT -q "SELECT count() FROM $CLICKHOUSE_DATABASE.t4")"
echo "4 ordinary->atomic reloaded $(reloaded_count "$CLICKHOUSE_DATABASE" t4)"

# 5. Atomic -> Atomic control. The path is keyed on the table UUID, which rename preserves, so
# this passes before the fix as well. It is here as a non-regression row, not as coverage.
make_table "$CLICKHOUSE_DATABASE" t5 12 "EmbeddedRocksDB"
$CLIENT -q "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}_at2"
$CLIENT -q "RENAME TABLE $CLICKHOUSE_DATABASE.t5 TO ${CLICKHOUSE_DATABASE}_at2.t5"
echo "5 atomic->atomic control reloaded $(reloaded_count "${CLICKHOUSE_DATABASE}_at2" t5)"

# 6. A table with TTL is reopened through rocksdb::DBWithTTL, so it needs its own row.
make_table "$CLICKHOUSE_DATABASE" t6 9 "EmbeddedRocksDB(3600)"
$CLIENT -q "RENAME TABLE $CLICKHOUSE_DATABASE.t6 TO $ORD1.t6"
echo "6 ttl reloaded $(reloaded_count "$ORD1" t6)"

# 7. A directory given explicitly in the engine arguments must NOT be moved: it does not belong
# to the table's location and may be shared.
mkdir -p "${EXPLICIT_DIR}"
$CLIENT -q --multiline "
    DROP TABLE IF EXISTS $CLICKHOUSE_DATABASE.t7 SYNC;
    CREATE TABLE $CLICKHOUSE_DATABASE.t7 (k UInt64, v String)
        ENGINE = EmbeddedRocksDB(0, '${EXPLICIT_DIR}/explicit7') PRIMARY KEY k;
    INSERT INTO $CLICKHOUSE_DATABASE.t7 SELECT number, toString(number) FROM numbers(8);
"
EXPLICIT_PATH_BEFORE=$($CLIENT -q "SELECT data_paths[1] FROM system.tables WHERE database = '$CLICKHOUSE_DATABASE' AND name = 't7'")
$CLIENT -q "RENAME TABLE $CLICKHOUSE_DATABASE.t7 TO $ORD1.t7"
# Read the path straight after the rename: a reload would recompute it from the engine argument
# again and hide a relocation that did happen.
EXPLICIT_PATH_AFTER=$($CLIENT -q "SELECT data_paths[1] FROM system.tables WHERE database = '$ORD1' AND name = 't7'")
if [ "$EXPLICIT_PATH_BEFORE" = "$EXPLICIT_PATH_AFTER" ]; then
    echo "7 explicit path unchanged 1"
else
    echo "7 explicit path unchanged 0 ($EXPLICIT_PATH_BEFORE -> $EXPLICIT_PATH_AFTER)"
fi
echo "7 explicit dir still there $(ls -d "${EXPLICIT_DIR}/explicit7" >/dev/null 2>&1 && echo 1 || echo 0)"
echo "7 explicit path reloaded $(reloaded_count "$ORD1" t7)"

# 8. Same skip branch for a read_only table, which is opened with OpenForReadOnly.
$CLIENT -q --multiline "
    DROP TABLE IF EXISTS $CLICKHOUSE_DATABASE.t8_writer SYNC;
    CREATE TABLE $CLICKHOUSE_DATABASE.t8_writer (k UInt64, v String)
        ENGINE = EmbeddedRocksDB(0, '${EXPLICIT_DIR}/explicit8') PRIMARY KEY k;
    INSERT INTO $CLICKHOUSE_DATABASE.t8_writer SELECT number, toString(number) FROM numbers(6);
    DROP TABLE $CLICKHOUSE_DATABASE.t8_writer SYNC SETTINGS ignore_drop_queries_probability = 0;
    CREATE TABLE $CLICKHOUSE_DATABASE.t8 (k UInt64, v String)
        ENGINE = EmbeddedRocksDB(0, '${EXPLICIT_DIR}/explicit8', 1) PRIMARY KEY k;
"
$CLIENT -q "RENAME TABLE $CLICKHOUSE_DATABASE.t8 TO $ORD1.t8"
echo "8 read_only explicit path reloaded $(reloaded_count "$ORD1" t8)"

# 9. ATTACH TABLE ... FROM is the other caller of IStorage::rename. Before the fix the data was
# left at the source directory; now it is moved into the table's own directory.
$CLIENT -q --multiline "
    DROP TABLE IF EXISTS $CLICKHOUSE_DATABASE.t9_seed SYNC;
    CREATE TABLE $CLICKHOUSE_DATABASE.t9_seed (k UInt64, v String)
        ENGINE = EmbeddedRocksDB(0, '${EXPLICIT_DIR}/attach_src') PRIMARY KEY k;
    INSERT INTO $CLICKHOUSE_DATABASE.t9_seed SELECT number, toString(number) FROM numbers(7);
    DROP TABLE $CLICKHOUSE_DATABASE.t9_seed SYNC SETTINGS ignore_drop_queries_probability = 0;
    DROP TABLE IF EXISTS $CLICKHOUSE_DATABASE.t9 SYNC;
"
$CLIENT -q "ATTACH TABLE $CLICKHOUSE_DATABASE.t9 FROM '${CLICKHOUSE_TEST_UNIQUE_NAME}/attach_src' (k UInt64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k"
echo "9 attach from count $($CLIENT -q "SELECT count() FROM $CLICKHOUSE_DATABASE.t9")"
echo "9 attach from moved $($CLIENT -q "SELECT data_paths[1] NOT LIKE '%${CLICKHOUSE_TEST_UNIQUE_NAME}%' FROM system.tables WHERE database = '$CLICKHOUSE_DATABASE' AND name = 't9'")"
# The live handle keeps answering from wherever it was opened, so only a reload proves the data
# is at the table's own directory and the source directory is empty.
echo "9 attach from reloaded $(reloaded_count "$CLICKHOUSE_DATABASE" t9)"
echo "9 source dir gone $(ls -d "${EXPLICIT_DIR}/attach_src" >/dev/null 2>&1 && echo 0 || echo 1)"

# 10. An existing destination directory must not be replaced, and a failed rename must leave the
# table attached and fully readable (the handle is restored, the caller re-attaches it).
make_table "$ORD1" t10 5 "EmbeddedRocksDB"
ORD1_DATA_DIR=$($CLIENT -q "SELECT substring(data_paths[1], 1, length(data_paths[1]) - length('t10/')) FROM system.tables WHERE database = '$ORD1' AND name = 't10'")
mkdir -p "${ORD1_DATA_DIR}t10_target"
$CLIENT -q "RENAME TABLE $ORD1.t10 TO $ORD1.t10_target" 2>&1 | grep -c -e "ATOMIC_RENAME_FAIL" -e "FILE_ALREADY_EXISTS" | sed 's/^/10 rename refused /'
echo "10 table still readable $($CLIENT -q "SELECT count() FROM $ORD1.t10")"
echo "10 table still reloadable $(reloaded_count "$ORD1" t10)"
rm -rf "${ORD1_DATA_DIR}t10_target"

# 11. A failure that is not a ClickHouse exception has to be handled too: the caller's catch-all
# re-attaches the table, and the storage restores its handle, so the table stays fully usable.
make_table "$ORD1" t11 4 "EmbeddedRocksDB"
$CLIENT -q "SYSTEM ENABLE FAILPOINT rocksdb_rename_throw_filesystem_error"
$CLIENT -q "RENAME TABLE $ORD1.t11 TO $ORD1.t11_target" 2>&1 | grep -c "injected" | sed 's/^/11 rename failed /'
$CLIENT -q "SYSTEM DISABLE FAILPOINT rocksdb_rename_throw_filesystem_error"
echo "11 table still readable $($CLIENT -q "SELECT count() FROM $ORD1.t11")"
echo "11 table still reloadable $(reloaded_count "$ORD1" t11)"

# 12. If restoring the handle also fails, the table stays attached under the old name with its data
# intact on disk but no usable handle. Reads must refuse instead of reporting zero rows, which is
# the very symptom this fix removes. The failpoint is REGULAR because both the forward reopen and
# the rollback reopen have to fail for that state to be reached, so the test disables it explicitly.
make_table "$ORD1" t12 3 "EmbeddedRocksDB"
$CLIENT -q "SYSTEM ENABLE FAILPOINT rocksdb_rename_fail_reopen"
$CLIENT -q "RENAME TABLE $ORD1.t12 TO $ORD1.t12_target" 2>&1 | grep -c "FAULT_INJECTED" | sed 's/^/12 rename failed /'
echo "12 full scan refuses $($CLIENT -q "SELECT * FROM $ORD1.t12" 2>&1 | grep -c "ROCKSDB_ERROR")"
echo "12 key lookup refuses $($CLIENT -q "SELECT * FROM $ORD1.t12 WHERE k = 1" 2>&1 | grep -c "ROCKSDB_ERROR")"
$CLIENT -q "SYSTEM DISABLE FAILPOINT rocksdb_rename_fail_reopen"
# The data was never moved, so a reload recovers the table completely.
echo "12 reloaded $(reloaded_count "$ORD1" t12)"

# 13. ATTACH TABLE ... FROM publishes the table and only then relocates its directory, so without
# a table lock a concurrent reader could hold an iterator into the RocksDB the relocation is about
# to close. The PAUSEABLE_ONCE failpoint stops the attach exactly in that window.
$CLIENT -q --multiline "
    DROP TABLE IF EXISTS $CLICKHOUSE_DATABASE.t13_seed SYNC;
    CREATE TABLE $CLICKHOUSE_DATABASE.t13_seed (k UInt64, v String)
        ENGINE = EmbeddedRocksDB(0, '${EXPLICIT_DIR}/attach_src13') PRIMARY KEY k;
    INSERT INTO $CLICKHOUSE_DATABASE.t13_seed SELECT number, toString(number) FROM numbers(5);
    DROP TABLE $CLICKHOUSE_DATABASE.t13_seed SYNC SETTINGS ignore_drop_queries_probability = 0;
    DROP TABLE IF EXISTS $CLICKHOUSE_DATABASE.t13 SYNC;
"
$CLIENT -q "SYSTEM ENABLE FAILPOINT attach_from_path_pause_before_relocation"
$CLIENT -q "ATTACH TABLE $CLICKHOUSE_DATABASE.t13 FROM '${CLICKHOUSE_TEST_UNIQUE_NAME}/attach_src13' (k UInt64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k" &
ATTACH_PID=$!
# Returns as soon as the attach has paused; no sleeping and no polling.
$CLIENT -q "SYSTEM WAIT FAILPOINT attach_from_path_pause_before_relocation PAUSE"
# A short lock timeout keeps this bounded: with the lock held the reader cannot get the table,
# without it the reader would read through the handle that is about to be closed.
CONCURRENT_READ=$($CLIENT --lock_acquire_timeout 1 -q "SELECT count() FROM $CLICKHOUSE_DATABASE.t13" 2>&1)
if [ "$CONCURRENT_READ" = "5" ]; then
    echo "13 concurrent read excluded 0"
else
    echo "13 concurrent read excluded 1"
fi
# DISABLE resumes the paused attach and removes the failpoint in one step.
$CLIENT -q "SYSTEM DISABLE FAILPOINT attach_from_path_pause_before_relocation"
wait $ATTACH_PID
echo "13 attach from count $($CLIENT -q "SELECT count() FROM $CLICKHOUSE_DATABASE.t13")"
echo "13 attach from reloaded $(reloaded_count "$CLICKHOUSE_DATABASE" t13)"

$CLIENT --force_remove_data_recursively_on_drop 1 -q --multiline "
    DROP TABLE IF EXISTS $CLICKHOUSE_DATABASE.t4 SYNC;
    DROP TABLE IF EXISTS $CLICKHOUSE_DATABASE.t9 SYNC;
    DROP TABLE IF EXISTS $CLICKHOUSE_DATABASE.t13 SYNC;
    DROP TABLE IF EXISTS $ORD1.t11 SYNC;
    DROP TABLE IF EXISTS $ORD1.t12 SYNC;
    DROP DATABASE IF EXISTS $ORD1 SYNC;
    DROP DATABASE IF EXISTS $ORD2 SYNC;
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_at2 SYNC;
"
rm -rf "${EXPLICIT_DIR}"

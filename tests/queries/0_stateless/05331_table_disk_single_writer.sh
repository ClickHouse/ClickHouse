#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-ordinary-database
# Tag no-fasttest: creates custom object-storage disks
# Tag no-replicated-database: every replica would run the CREATE and mount the same disk root
# Tag no-shared-merge-tree: the parts found under the root are not the part set SharedMergeTree reads
# Tag no-ordinary-database: the drop and detach arms need the deferred drop queue and detached instances of Atomic

# A table with `table_disk` keeps its data at the root of its disk, so a writable disk can hold the data of
# only one table: a second writable table on the same root is refused, read-only tables are allowed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_path="${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"

function table_def()
{
    local name=$1 disk_name=$2 extra=$3
    echo "$name (k Int32) ENGINE = MergeTree ORDER BY k
    SETTINGS table_disk = true, disk = disk(
        name = '${disk_name}', ${extra}
        type = object_storage, object_storage_type = local, metadata_type = plain_rewritable, path = '${disk_path}')"
}

writer_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_writer"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_def writer "${writer_disk}")"

echo '-- a second writable table on the same root is refused'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_def second "${writer_disk}")" 2>&1 | grep -o -m1 'can hold the data of only one table.*(BAD_ARGUMENTS)'
uuid=$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE $(table_def "second UUID '${uuid}'" "${writer_disk}")" 2>&1 | grep -o -m1 'can hold the data of only one table.*(BAD_ARGUMENTS)'

echo '-- a read-only table on the same root is allowed'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_def reader "${CLICKHOUSE_TEST_UNIQUE_NAME}_reader" 'read_only = true,')"

${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES (1)"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM writer"

echo '-- the same table can be attached again after a detach'
${CLICKHOUSE_CLIENT} --query "DETACH TABLE writer SYNC"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE writer"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM writer"

echo '-- an ALTER that keeps the disk is allowed'
${CLICKHOUSE_CLIENT} --query "ALTER TABLE writer MODIFY SETTING old_parts_lifetime = 100"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM writer"

echo '-- after the drop another table can use the root'
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_def second "${writer_disk}")"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM second"

${CLICKHOUSE_CLIENT} --query "DROP TABLE second SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE reader SYNC"

# A wrapper repeats the definition of the disk it wraps: a table that names a custom disk
# cannot be loaded after a restart without a table that defines it.
function plain_disk()
{
    local name=$1
    echo "disk(name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}', type = object_storage, object_storage_type = local,
        metadata_type = plain_rewritable, path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}/')"
}

function table_on()
{
    echo "$1 (k Int32) ENGINE = MergeTree ORDER BY k SETTINGS table_disk = true, disk = $2"
}

no_sync="--database_atomic_wait_for_drop_and_detach_synchronously=0"

echo '-- a table dropped without SYNC keeps the root until the drop finishes'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_on pending "$(plain_disk pending)")"
${CLICKHOUSE_CLIENT} ${no_sync} --query "DROP TABLE pending"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_on after_pending "$(plain_disk pending)")" 2>&1 | grep -o -m1 'It is dropped.*(BAD_ARGUMENTS)'

echo '-- a table detached without SYNC keeps the root while its detached instance is alive'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_on detached "$(plain_disk detached)")"
${CLICKHOUSE_CLIENT} ${no_sync} --query "DETACH TABLE detached"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_on after_detached "$(plain_disk detached)")" 2>&1 | grep -o -m1 'It is detached.*(BAD_ARGUMENTS)'
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE detached"
${CLICKHOUSE_CLIENT} --query "DROP TABLE detached SYNC"

echo '-- two cache disks over one writable disk share its root'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_on c1 "disk(name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_c1', type = cache,
    path = 'filesystem_caches/${CLICKHOUSE_TEST_UNIQUE_NAME}_c1/', max_size = '1Gi', disk = $(plain_disk inner))")"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_on c2 "disk(name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_c2', type = cache,
    path = 'filesystem_caches/${CLICKHOUSE_TEST_UNIQUE_NAME}_c2/', max_size = '1Gi', disk = $(plain_disk inner))")" 2>&1 | grep -o -m1 'can hold the data of only one table.*(BAD_ARGUMENTS)'
${CLICKHOUSE_CLIENT} --query "DROP TABLE c1 SYNC"

echo '-- encrypted disks over sibling paths of one disk are allowed, nested and parent paths are refused'
function encrypted_disk()
{
    local name=$1 path=$2 delegate_def=$3
    echo "disk(name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}', type = encrypted, key = '1234567812345678', path = '${path}',
        disk = ${delegate_def})"
}
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_on ea "$(encrypted_disk ea 'a/' "$(plain_disk delegate)")")"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_on eb "$(encrypted_disk eb 'b/' "$(plain_disk delegate)")")"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ea VALUES (1)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO eb VALUES (2)"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ea"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM eb"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_on en "$(encrypted_disk en 'a/n/' "$(plain_disk delegate)")")" 2>&1 | grep -o -m1 'can hold the data of only one table.*(BAD_ARGUMENTS)'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_on ep "$(plain_disk delegate)")" 2>&1 | grep -o -m1 'can hold the data of only one table.*(BAD_ARGUMENTS)'
${CLICKHOUSE_CLIENT} --query "DROP TABLE ea SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE eb SYNC"

echo '-- two tables on one write-once disk are allowed'
write_once="${CLICKHOUSE_TEST_UNIQUE_NAME}_plain"
write_once_disk="disk(name = '${write_once}', type = object_storage, object_storage_type = local, metadata_type = plain,
    path = '${CLICKHOUSE_DISKS_FILES}/${write_once}/')"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_on w1 "${write_once_disk}")"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $(table_on w2 "${write_once_disk}")"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM w2"
${CLICKHOUSE_CLIENT} --query "DROP TABLE w1 SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE w2 SYNC"

echo '-- clickhouse-local, default database'
work_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local"
rm -rf "${work_dir}"
mkdir -p "${work_dir}"

function local_table_def()
{
    local name=$1 root=$2
    echo "$name (k Int32) ENGINE = MergeTree ORDER BY k
    SETTINGS table_disk = true, disk = disk(
        name = '${root}', type = object_storage, object_storage_type = local, metadata_type = plain_rewritable,
        path = '${work_dir}/${root}/')"
}

${CLICKHOUSE_LOCAL} --path "${work_dir}/db" --query "
CREATE TABLE $(local_table_def t root_t);
INSERT INTO t VALUES (1);
DETACH TABLE t;
ATTACH TABLE t;
SELECT * FROM t;
CREATE TABLE $(local_table_def e root_e);
DETACH TABLE e SYNC;
CREATE TABLE $(local_table_def e2 root_e);
SELECT count() FROM e2;
" -- --custom_local_disks_base_directory="${work_dir}/"

echo '-- the disk of the table cannot be changed'
${CLICKHOUSE_LOCAL} --path "${work_dir}/alter" --query "
CREATE TABLE $(local_table_def a root_a);
ALTER TABLE a RESET SETTING disk;
" -- --custom_local_disks_base_directory="${work_dir}/" 2>&1 | grep -o -m1 'cannot be changed.*(BAD_ARGUMENTS)'

rm -rf "${work_dir}"

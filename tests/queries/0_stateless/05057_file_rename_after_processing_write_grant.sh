#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: that mode installs a replicated access storage, whose entity serialization
# round trip widens `GRANT READ ON FILE` to both `READ` and `WRITE` (see issue
# https://github.com/ClickHouse/ClickHouse/issues/111402, whose fix is not merged yet), so the reader
# would hold `WRITE` and none of the denials asserted below would fire.

# `rename_files_after_processing` renames the files a `SELECT` has read, so it is a write to the
# source and requires `WRITE ON FILE` on top of `READ ON FILE`. It is also a per-query rule: in a
# `Filesystem` database it must not reach another query, and it must be shared within its own.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILES_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
mkdir -p "${FILES_DIR}"

READER="reader_${CLICKHOUSE_TEST_UNIQUE_NAME}"
FS_DB="fsdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
URL_DB="urldb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
RENAME="rename_files_after_processing='processed_%a'"

# One input file per renaming scenario: a successful rename consumes the name.
for name in direct_select wrapped_url cluster_initiator cluster_no_setting cluster_granted \
            explain_pipeline explain_plan granted_write urldb_denied urldb_granted \
            cached_armed_for_reader cached_armed_for_owner cached_unarmed \
            repeated_ref repeated_ref_setting \
            cluster_unoptimized dist_insert_denied dist_insert_granted; do
    echo 7 > "${FILES_DIR}/${name}.csv"
done

${CLICKHOUSE_CLIENT} -q "
DROP DATABASE IF EXISTS ${FS_DB};
DROP DATABASE IF EXISTS ${URL_DB};
DROP USER IF EXISTS ${READER};
CREATE USER ${READER} IDENTIFIED WITH no_password;
GRANT CREATE TEMPORARY TABLE ON *.* TO ${READER};
GRANT READ ON FILE TO ${READER};
"

# Prints what became of one input file. The no-side-effect half of every denial is asserted with it:
# a refusal that still renamed the file would regress silently.
file_state() {
    if [ -f "${FILES_DIR}/processed_$1.csv" ] && [ ! -f "${FILES_DIR}/$1.csv" ]; then
        echo "renamed"
    elif [ -f "${FILES_DIR}/$1.csv" ] && [ ! -f "${FILES_DIR}/processed_$1.csv" ]; then
        echo "intact"
    else
        echo "unexpected"
    fi
}

echo '--- the read-only grant set really is read-only: an explicit write is refused'
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "INSERT INTO FUNCTION file('${FILES_DIR}/direct_select.csv', 'CSV', 'x UInt8') SELECT 9" 2>&1 |
    grep -o -m1 'WRITE ON FILE'

echo '--- a renaming SELECT through file() is refused, and renames nothing'
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "SELECT * FROM file('${FILES_DIR}/direct_select.csv', 'CSV', 'x UInt8') SETTINGS ${RENAME}" 2>&1 |
    grep -o -m1 'WRITE ON FILE'
file_state direct_select

echo '--- the same through url(file://), which resolves to a file() delegate'
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "SELECT * FROM url('file://${FILES_DIR}/wrapped_url.csv', 'CSV', 'x UInt8') SETTINGS ${RENAME}" 2>&1 |
    grep -o -m1 'WRITE ON FILE'
file_state wrapped_url

echo '--- the same through fileCluster(), refused on the initiator'
# Without a cluster secret the secondary query is authorized as the cluster's configured user, so a
# worker-side check alone would let this user rename the file.
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "SELECT * FROM fileCluster('test_shard_localhost', '${FILES_DIR}/cluster_initiator.csv', 'CSV', 'x UInt8') SETTINGS ${RENAME}" 2>&1 |
    grep -o -m1 'WRITE ON FILE'
file_state cluster_initiator

echo '--- and through fileCluster() even when no plan optimization runs, which builds no task iterator'
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "EXPLAIN PLAN optimize = 0 SELECT * FROM
     fileCluster('test_shard_localhost', '${FILES_DIR}/cluster_unoptimized.csv', 'CSV', 'x UInt8')
     SETTINGS ${RENAME}" 2>&1 |
    grep -o -m1 'WRITE ON FILE'
file_state cluster_unoptimized

echo '--- EXPLAIN PIPELINE builds the readers, so it renames too, and is refused'
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "EXPLAIN PIPELINE SELECT * FROM file('${FILES_DIR}/explain_pipeline.csv', 'CSV', 'x UInt8') SETTINGS ${RENAME}" 2>&1 |
    grep -o -m1 'WRITE ON FILE'
file_state explain_pipeline

echo '--- EXPLAIN PLAN renames nothing, but needs the privileges of the query it explains'
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "EXPLAIN PLAN SELECT * FROM file('${FILES_DIR}/explain_plan.csv', 'CSV', 'x UInt8') SETTINGS ${RENAME}" 2>&1 |
    grep -o -m1 'WRITE ON FILE'
file_state explain_plan

echo '--- structure resolution builds no reader and keeps working with READ ON FILE alone'
# The resolved structure is printed, not just the absence of an error: a count of missing denials
# would read the same whether the DESCRIBE succeeded or failed for an unrelated reason.
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "DESCRIBE TABLE file('${FILES_DIR}/explain_plan.csv', 'CSV', 'x UInt8') SETTINGS ${RENAME} FORMAT TSV" 2>&1 |
    cut -f1,2 | head -1
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "DESCRIBE TABLE fileCluster('test_shard_localhost', '${FILES_DIR}/explain_plan.csv', 'CSV', 'x UInt8')
     SETTINGS describe_include_virtual_columns = 1, ${RENAME} FORMAT TSV" 2>&1 |
    cut -f1,2 | head -1
file_state explain_plan

echo '--- a plain read without the setting is not affected'
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "SELECT * FROM file('${FILES_DIR}/cached_unarmed.csv', 'CSV', 'x UInt8')"
file_state cached_unarmed

echo '--- and neither is a fileCluster read without the setting'
# The initiator check keys on the setting: made unconditional it would refuse every `fileCluster`
# read to a user holding only `READ ON FILE`.
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "SELECT * FROM fileCluster('test_shard_localhost', '${FILES_DIR}/cluster_no_setting.csv', 'CSV', 'x UInt8')"
file_state cluster_no_setting

echo '--- a distributed INSERT SELECT is refused too, and inserts nothing'
# `parallel_distributed_insert_select` hands the workers their tasks straight from
# `getTaskIteratorExtension`, so this route never reaches `IStorageCluster::read`. It is pinned
# because the runner randomizes it to 0, which would take the two arms below the other route.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE ${CLICKHOUSE_DATABASE}.dst (x UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE ${CLICKHOUSE_DATABASE}.dist AS ${CLICKHOUSE_DATABASE}.dst
    ENGINE = Distributed('test_shard_localhost', '${CLICKHOUSE_DATABASE}', 'dst');
GRANT INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${READER};
"
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "INSERT INTO ${CLICKHOUSE_DATABASE}.dist SELECT * FROM
     fileCluster('test_shard_localhost', '${FILES_DIR}/dist_insert_denied.csv', 'CSV', 'x UInt8')
     SETTINGS ${RENAME}, parallel_distributed_insert_select = 2" 2>&1 |
    grep -o -m1 'WRITE ON FILE'
file_state dist_insert_denied
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${CLICKHOUSE_DATABASE}.dst"

echo '--- a DESCRIBE with the rule set arms nothing in the Filesystem cache for a later query'
${CLICKHOUSE_CLIENT} -q "
CREATE DATABASE ${FS_DB} ENGINE = Filesystem('${FILES_DIR}');
GRANT SELECT ON ${FS_DB}.* TO ${READER};
"
# A DESCRIBE with the setting used to leave an armed table in the database cache, so an unrelated
# later query renamed the file: the reader never asked for it, and the owner never authorized it.
${CLICKHOUSE_CLIENT} -q \
    "DESCRIBE TABLE ${FS_DB}.\`cached_armed_for_reader.csv\` SETTINGS ${RENAME}" > /dev/null
${CLICKHOUSE_CLIENT} --user "${READER}" -q "SELECT * FROM ${FS_DB}.\`cached_armed_for_reader.csv\`"
file_state cached_armed_for_reader

${CLICKHOUSE_CLIENT} -q \
    "DESCRIBE TABLE ${FS_DB}.\`cached_armed_for_owner.csv\` SETTINGS ${RENAME}" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${FS_DB}.\`cached_armed_for_owner.csv\`"
file_state cached_armed_for_owner

echo '--- and it does not serve a renaming query from an entry cached without the rule'
# The mirror image: a warm unarmed entry used to make the setting a silent no-op, so the rename never
# happened and no privilege was required for asking.
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${FS_DB}.\`cached_unarmed.csv\`" > /dev/null
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "SELECT * FROM ${FS_DB}.\`cached_unarmed.csv\` SETTINGS ${RENAME}" 2>&1 |
    grep -o -m1 'WRITE ON FILE'
file_state cached_unarmed
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${FS_DB}.\`cached_unarmed.csv\` SETTINGS ${RENAME}"
file_state cached_unarmed

echo '--- two references to one armed table in a query read the same table, renamed once at the end'
# A reference resolved to its own table would rename the file while the other one still has it open,
# and that one then fails to find it. Both counts are printed, so a reader losing its rows shows up
# here rather than as a missing rename.
${CLICKHOUSE_CLIENT} -q "SELECT (SELECT count() FROM ${FS_DB}.\`repeated_ref.csv\`) AS from_scalar,
                                count() AS from_outer FROM ${FS_DB}.\`repeated_ref.csv\` SETTINGS ${RENAME}"
file_state repeated_ref

echo '--- and they still share it when one of them also changed an unrelated setting of its own'
# The per-query memo keys on the changed settings of the resolving context, so a local change that has
# nothing to do with renaming must not put the two references on separate tables.
${CLICKHOUSE_CLIENT} -q "SELECT (SELECT count() FROM ${FS_DB}.\`repeated_ref_setting.csv\` SETTINGS max_threads = 2) AS from_scalar,
                                count() AS from_outer FROM ${FS_DB}.\`repeated_ref_setting.csv\` SETTINGS ${RENAME}"
file_state repeated_ref_setting

echo '--- a URL database over file:// is gated the same way'
# Its own resolution path: the delegate is built on a `Context::createCopy`, so the rule survives and
# arms the storage, and the read reaches the gate through `StorageURLDatabaseTable::read`. The base
# is empty and the table name is the absolute path, as in `04658_url_database_structure_read_grant`.
${CLICKHOUSE_CLIENT} -q "
CREATE DATABASE ${URL_DB} ENGINE = URL('file://');
GRANT SELECT ON ${URL_DB}.* TO ${READER};
"
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "SELECT * FROM ${URL_DB}.\`${FILES_DIR}/urldb_denied.csv\` SETTINGS ${RENAME}" 2>&1 |
    grep -o -m1 'WRITE ON FILE'
file_state urldb_denied
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${URL_DB}.\`${FILES_DIR}/urldb_granted.csv\` SETTINGS ${RENAME}"
file_state urldb_granted

echo '--- with WRITE ON FILE granted, the rename happens'
${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON FILE TO ${READER}"
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "SELECT * FROM file('${FILES_DIR}/granted_write.csv', 'CSV', 'x UInt8') SETTINGS ${RENAME}"
file_state granted_write

echo '--- and the fileCluster workers still perform it once the initiator is authorized'
# The initiator check is not the rename: it throws before any worker is contacted, so without this
# arm the workers could stop applying the rule and every other `fileCluster` arm would still pass.
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "SELECT * FROM fileCluster('test_shard_localhost', '${FILES_DIR}/cluster_granted.csv', 'CSV', 'x UInt8') SETTINGS ${RENAME}"
file_state cluster_granted

echo '--- as does a distributed INSERT SELECT, which also delivers its rows'
# Emptied first so the count below is this arm's own: a regression that let the refused insert
# through would otherwise show up here rather than where it happened.
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE ${CLICKHOUSE_DATABASE}.dst"
${CLICKHOUSE_CLIENT} --user "${READER}" -q \
    "INSERT INTO ${CLICKHOUSE_DATABASE}.dist SELECT * FROM
     fileCluster('test_shard_localhost', '${FILES_DIR}/dist_insert_granted.csv', 'CSV', 'x UInt8')
     SETTINGS ${RENAME}, parallel_distributed_insert_select = 2"
file_state dist_insert_granted
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${CLICKHOUSE_DATABASE}.dst"

${CLICKHOUSE_CLIENT} -q "
DROP DATABASE IF EXISTS ${FS_DB};
DROP DATABASE IF EXISTS ${URL_DB};
DROP USER IF EXISTS ${READER};
"
rm -rf "${FILES_DIR}"

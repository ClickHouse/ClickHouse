#!/usr/bin/env bash
# Tags: no-fasttest, use-rocksdb, no-replicated-database
# no-fasttest: EmbeddedRocksDB requires libraries
# no-replicated-database: on a replicated / shared-catalog database the DDL runs with no user, so the
# in-storage FILE check every denied arm here relies on is a no-op and the deny path silently allows.
# Blocked on https://github.com/ClickHouse/ClickHouse/issues/111561 - re-enable when fixed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `EmbeddedRocksDB(ttl, rocksdb_dir, read_only)` with an explicit directory opens a directory under
# `user_files`, so it needs the same `FILE` source grant as `file`: `READ` to open one, and `WRITE` as
# well whenever the statement could create it — a writable table, or a read_only one whose directory
# does not exist yet, since the CREATE path runs `fs::create_directories` before the read-only open. The argument-less form touches only the table's own data directory and
# stays free, and a definition replayed from stored metadata is not re-checked.

# A user, a database, three user_files directories and a backup destination are global names, and the
# flaky check runs many copies of one test at once, so they carry the pid as well: two live copies never
# share one. The unique name alone is stable across runs, and a backup may not be written twice to one
# destination.
NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_$$"
USER="low_${NAME}"
POC="${CLICKHOUSE_DATABASE}_poc_$$"
# Leading letter: an identifier starting with a digit comes back backticked in the privilege sentence,
# which the assertion below spells without backticks.
VICTIM="${CLICKHOUSE_DATABASE}.victim_${NAME}"
SECRET_DIR="${NAME}_secret"
RW_DIR="${NAME}_rw"
RESTORE_DIR="${NAME}_restore"
MISSING_DIR="${NAME}_missing"
BACKUP="Disk('backups', '05054_${NAME}')"

# The denial assertions match the privilege sentence rather than the grant name: the client echoes the
# failing query back, and this test's own object names contain both "file" and "grant", so a looser
# pattern would match that echo instead of a denial.
denied_read() { grep -qF "necessary to have the grant READ ON FILE"; }
# Holding `READ` already is what makes the writable arm unambiguous: only `WRITE` is then reported
# missing, whereas a caller missing both is told about them together with no "Missing permissions".
denied_write() { grep -qF "Missing permissions: WRITE ON FILE"; }

# The absence of a denial is not success: a parse error or a table left behind by an interrupted run
# leaves the same empty match. So every arm expecting a statement to be permitted also asserts the
# table is there, queried as the admin so a missing SELECT grant cannot hide the row, and after the
# statement rather than before, since a detached table has no row here either. A failure of the probe
# itself is reported rather than read as absence, which the denial arms would accept as their pass.
created() {
    local count rc
    count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${POC}' AND name = '$1'")
    rc=$?
    if [ "$rc" -ne 0 ] || { [ "$count" != "0" ] && [ "$count" != "1" ]; }; then
        echo "created-probe-FAILED (unexpected) for $1: rc=$rc out='$count'"
        return 2
    fi
    [ "$count" = "1" ]
}

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${POC}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${POC}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE, SELECT, INSERT ON ${POC}.* TO ${USER}"
# The test config sets table_engines_require_grant, so naming the engine is a precondition every arm needs.
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON EmbeddedRocksDB TO ${USER}"

# The victim is an admin table in another database, holding a row ${USER} has no grant to read.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${VICTIM} (k String, v String) ENGINE = EmbeddedRocksDB(0, '${SECRET_DIR}', 0) PRIMARY KEY k SETTINGS optimize_for_bulk_insert = 0"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${VICTIM} VALUES ('marker', 'secret')"

echo "--- the victim rows are protected: reading the table itself is denied ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${VICTIM}" 2>&1 \
    | grep -qF "necessary to have the grant SELECT ON ${VICTIM}" && echo "select-denied" || echo "NOT DENIED"

echo "--- a read_only table over the victim directory without READ ON FILE is denied ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${POC}.leak (k String, v String) ENGINE = EmbeddedRocksDB(0, '${SECRET_DIR}', 1) PRIMARY KEY k" 2>&1 \
    | denied_read && echo "read-only-denied" || echo "NOT DENIED"
created leak && echo "CREATED ANYWAY (unexpected)" || echo "not-created"

echo "--- with READ ON FILE the same statement is allowed, sharing the live directory ---"
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO ${USER}"
out=$(${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${POC}.leak (k String, v String) ENGINE = EmbeddedRocksDB(0, '${SECRET_DIR}', 1) PRIMARY KEY k" 2>&1)
rc=$?
if [ "$rc" -eq 0 ] && created leak; then echo "read-only-allowed"; else echo "read-only-FAILED (unexpected): rc=$rc $out"; fi
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT * FROM ${POC}.leak"

echo "--- a read_only table over a missing directory needs WRITE and does not create it ---"
# The CREATE path runs `fs::create_directories(rocksdb_dir)` before the read-only open, so authorizing
# this with READ alone would let a read-only source grant leave a new directory in `user_files`.
${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${POC}.missing (k String, v String) ENGINE = EmbeddedRocksDB(0, '${MISSING_DIR}', 1) PRIMARY KEY k" 2>&1 \
    | denied_write && echo "missing-dir-denied" || echo "NOT DENIED"
created missing && echo "CREATED ANYWAY (unexpected)" || echo "not-created"
if [ -n "${CLICKHOUSE_USER_FILES}" ] && [ -e "${CLICKHOUSE_USER_FILES:?}/${MISSING_DIR}" ]; then
    echo "DIRECTORY CREATED (unexpected)"
else
    echo "dir-absent"
fi

echo "--- a writable table needs WRITE ON FILE as well ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${POC}.rw (k String, v String) ENGINE = EmbeddedRocksDB(0, '${RW_DIR}') PRIMARY KEY k" 2>&1 \
    | denied_write && echo "writable-denied" || echo "NOT DENIED"
created rw && echo "CREATED ANYWAY (unexpected)" || echo "not-created"
${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON FILE TO ${USER}"
out=$(${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${POC}.rw (k String, v String) ENGINE = EmbeddedRocksDB(0, '${RW_DIR}') PRIMARY KEY k" 2>&1)
rc=$?
if [ "$rc" -eq 0 ] && created rw; then echo "writable-allowed"; else echo "writable-FAILED (unexpected): rc=$rc $out"; fi

echo "--- the argument-less form has no user_files exposure and needs no grant ---"
${CLICKHOUSE_CLIENT} -q "REVOKE READ, WRITE ON FILE FROM ${USER}"
out=$(${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${POC}.own (k String, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k" 2>&1)
rc=$?
if [ "$rc" -eq 0 ] && created own; then echo "default-form-allowed"; else echo "default-form-FAILED (unexpected): rc=$rc $out"; fi
out=$(${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${POC}.own_ttl (k String, v String) ENGINE = EmbeddedRocksDB(0) PRIMARY KEY k" 2>&1)
rc=$?
if [ "$rc" -eq 0 ] && created own_ttl; then echo "ttl-only-form-allowed"; else echo "ttl-only-form-FAILED (unexpected): rc=$rc $out"; fi

echo "--- a short ATTACH replays stored metadata and stays loadable after a revoke ---"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${POC}.leak"
out=$(${CLICKHOUSE_CLIENT} --user "${USER}" -q "ATTACH TABLE ${POC}.leak" 2>&1)
rc=$?
if [ "$rc" -eq 0 ] && created leak; then echo "short-attach-allowed"; else echo "short-attach-FAILED (unexpected): rc=$rc $out"; fi

echo "--- an ATTACH carrying a full definition is checked like CREATE ---"
# Attaching a full definition into an Atomic database (the default) requires an explicit UUID, derived
# from the per-copy name above so two live copies cannot ask for the same one. `sipHash128` rather than
# `MD5`: `MD5` throws `SUPPORT_IS_DISABLED` in OpenSSL FIPS builds.
UUID=$(${CLICKHOUSE_CLIENT} -q "SELECT reinterpretAsUUID(sipHash128('${NAME}_attached'))")
${CLICKHOUSE_CLIENT} --user "${USER}" -q "ATTACH TABLE ${POC}.attached UUID '${UUID}' (k String, v String) ENGINE = EmbeddedRocksDB(0, '${SECRET_DIR}', 1) PRIMARY KEY k" 2>&1 \
    | denied_read && echo "full-attach-denied" || echo "NOT DENIED"
created attached && echo "CREATED ANYWAY (unexpected)" || echo "not-created"

echo "--- a RESTORE carries a fresh definition and is checked like CREATE ---"
# A RESTORE supplies a definition under whoever is restoring, not one this server stored, so it is
# checked rather than replayed.
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO ${USER}"
# Reading a Disk(...) backup location needs READ ON DISK, and that check runs before the
# WRITE ON FILE one this arm asserts on. Grant it up front so the denial below is the
# EmbeddedRocksDB FILE check and not the backup location's own.
${CLICKHOUSE_CLIENT} -q "GRANT READ ON DISK TO ${USER}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${POC}.restore_src (k String, v String) ENGINE = EmbeddedRocksDB(0, '${RESTORE_DIR}') PRIMARY KEY k"
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE ${POC}.restore_src TO ${BACKUP} FORMAT Null"
# A top-level `DROP TABLE` of a table that stores data on disk is ignored with the probability the
# stress runner puts in `ignore_drop_queries_probability`, and still reports success. Pin it to 0
# wherever this test relies on a drop having happened: the `RESTORE` below needs the name free.
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${POC}.restore_src SYNC SETTINGS ignore_drop_queries_probability = 0"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "RESTORE TABLE ${POC}.restore_src FROM ${BACKUP} FORMAT Null" 2>&1 \
    | denied_write && echo "restore-denied" || echo "NOT DENIED"
created restore_src && echo "CREATED ANYWAY (unexpected)" || echo "not-created"
${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON FILE TO ${USER}"
out=$(${CLICKHOUSE_CLIENT} --user "${USER}" -q "RESTORE TABLE ${POC}.restore_src FROM ${BACKUP} FORMAT Null" 2>&1)
rc=$?
if [ "$rc" -eq 0 ] && created restore_src; then echo "restore-allowed"; else echo "restore-FAILED (unexpected): rc=$rc $out"; fi

# `IF EXISTS` is not used on either drop: it reports success for a merely detached object, whose
# metadata still names these directories. Their output is discarded because any stderr fails the test.
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${POC} SYNC" 2>/dev/null
poc_rc=$?
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${VICTIM} SYNC SETTINGS ignore_drop_queries_probability = 0" 2>/dev/null
victim_rc=$?
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"

# A drop that reports success and leaves the table behind is the silent skip above, not a lost race,
# so it is reported rather than trusted. A failed probe is not read as absence: the directories stay.
victim_left=$(${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${VICTIM}")
probe_rc=$?
if [ "$victim_rc" -eq 0 ] && [ "$probe_rc" -eq 0 ] && [ "$victim_left" != "0" ]; then
    echo "victim-survived-a-successful-drop (unexpected)"
fi

# Dropping such a table closes its handle but leaves the directory, so remove the three this test made,
# once the drops above have reported success and the victim is really gone: a `read_only` definition
# whose `rocksdb_dir` is gone cannot be attached, and a writable one silently comes back empty.
if [ -n "${CLICKHOUSE_USER_FILES}" ] && [ "$poc_rc" -eq 0 ] && [ "$victim_rc" -eq 0 ] \
   && [ "$probe_rc" -eq 0 ] && [ "$victim_left" = "0" ]; then
    rm -rf "${CLICKHOUSE_USER_FILES:?}/${SECRET_DIR}" "${CLICKHOUSE_USER_FILES:?}/${RW_DIR}" \
        "${CLICKHOUSE_USER_FILES:?}/${RESTORE_DIR}" "${CLICKHOUSE_USER_FILES:?}/${MISSING_DIR}"
fi

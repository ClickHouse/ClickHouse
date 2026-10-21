#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: dispatches to the S3/Azure object-storage backends (not built in the fast-test image)
# and relies on the `table_engines_require_grant` access-control improvement enabled for the
# stateless test server.
# no-replicated-database: on a replicated / shared-catalog database the DDL runs with no user, so the
# in-storage access check these engine-denial assertions rely on is a no-op and they silently allow.
# Blocked on https://github.com/ClickHouse/ClickHouse/issues/111561 - re-enable when fixed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The dispatch re-checks `TABLE ENGINE` on the *target* backend whenever a definition is freshly
# supplied by the user: a user granted only `TABLE ENGINE ON URL` can create http(s) URL tables
# (served by the URL engine itself) but is denied `s3://`/`az://`/`file://` tables, which dispatch to
# `S3`/`AzureBlobStorage`/`File` and require those engine grants. The denial naming the target engine
# proves the dispatched backend (not the plain URL engine) is the one created. The http case is
# asserted here too, so a denial cannot pass merely because the user holds no engine grant at all.
# A full-definition `ATTACH` is such a fresh definition and is checked the same way, while a short
# `ATTACH TABLE t` replays a definition already stored on this server and is not re-checked.

S3_URL="s3://my-bucket/my-key.csv"

# Attaching a full definition into an Atomic database (the default) requires an explicit UUID. Each
# one is derived from the test's unique name plus a per-table suffix: two tables may never share a
# UUID, and concurrent copies of this test must not collide with each other. `sipHash128` rather
# than `MD5`: `MD5` throws `SUPPORT_IS_DISABLED` in OpenSSL FIPS builds.
attach_uuid() { ${CLICKHOUSE_CLIENT} -q "SELECT reinterpretAsUUID(sipHash128('${CLICKHOUSE_TEST_UNIQUE_NAME}_$1'))"; }

# The denial assertions match the privilege sentence, not just the engine name: the client echoes the
# failing query back, and that echo contains both the engine name and the word "grant" (it is in this
# test's own name), so a looser pattern would match a query that was never denied at all.
denied_on() { grep -qiF "necessary to have the grant TABLE ENGINE ON $1"; }

# The absence of a denial is not success. A parse error, a UUID collision or any other failure leaves
# the same empty match, so every arm asserting a statement was permitted also requires exit status 0
# and asserts that the table is there. Queried as the admin, so a missing `SELECT` grant cannot hide
# the row, and after the statement rather than before, since a detached table has no row here either.
# A failure of this probe itself is reported instead of read as absence, which the arms expecting
# nothing to have been created would otherwise accept as their pass.
attached() {
    local count rc
    count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = '$1'")
    rc=$?
    if [ "$rc" -ne 0 ] || { [ "$count" != "0" ] && [ "$count" != "1" ]; }; then
        echo "attached-probe-FAILED (unexpected) for $1: rc=$rc out='$count'"
        return 2
    fi
    [ "$count" = "1" ]
}

USER="url_only_${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE, DROP TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${USER}"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON URL TO ${USER}"
# The RESTORE arms below read from a Disk(...) locator, which requires the DISK source grant.
${CLICKHOUSE_CLIENT} -q "GRANT READ ON DISK TO ${USER}"

echo "--- TABLE ENGINE ON URL grant alone: ENGINE = URL('http://...') is allowed (URL engine) ---"
# Dropped as the admin, so the precondition holds whatever the test user's grants are at this point.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_http SYNC"
out=$(${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_http (a UInt32) ENGINE = URL('http://example.com/data.csv', 'CSV')" 2>&1)
rc=$?
if echo "$out" | grep -qiE "Not enough privileges|ACCESS_DENIED"; then
    echo "http-DENIED (unexpected)"
elif [ "$rc" -eq 0 ] && attached "${CLICKHOUSE_TEST_UNIQUE_NAME}_http"; then
    echo "http-allowed"
else
    echo "http-FAILED (unexpected): rc=$rc $out"
fi
${CLICKHOUSE_CLIENT} --user "${USER}" -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_http"

echo "--- TABLE ENGINE ON URL grant alone: ENGINE = URL('s3://...') is denied (dispatches to S3) ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_d_s3 (a UInt32) ENGINE = URL('${S3_URL}', 'CSV')" 2>&1 \
    | denied_on S3 && echo "s3-engine-denied" || echo "NOT DENIED"

echo "--- TABLE ENGINE ON URL grant alone: ENGINE = URL('az://...') is denied (dispatches to AzureBlobStorage) ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_d_az (a UInt32) ENGINE = URL('az://account.blob.core.windows.net/container/blob.csv', 'CSV')" 2>&1 \
    | denied_on AzureBlobStorage && echo "azure-engine-denied" || echo "NOT DENIED"

echo "--- ATTACH carrying a full definition is checked like CREATE: URL('file://...') is denied (dispatches to File) ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_a_file UUID '$(attach_uuid a_file)' (s String) ENGINE = URL('file://${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_a.csv', 'LineAsString')" 2>&1 \
    | denied_on File && echo "attach-file-engine-denied" || echo "NOT DENIED"

echo "--- ATTACH carrying a full definition is checked like CREATE: URL('az://...') is denied (dispatches to AzureBlobStorage) ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_a_az UUID '$(attach_uuid a_az)' (a UInt32) ENGINE = URL('az://account.blob.core.windows.net/container/blob.csv', 'CSV')" 2>&1 \
    | denied_on AzureBlobStorage && echo "attach-azure-engine-denied" || echo "NOT DENIED"

echo "--- ATTACH carrying a full definition: URL('http://...') is still allowed (URL engine) ---"
# A table left behind by an interrupted run would make this ATTACH fail `TABLE_ALREADY_EXISTS` while
# still leaving the name present, so the target is removed first and its absence is what makes the
# post-statement presence below evidence that this statement is the one that created it.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_a_http SYNC"
out=$(${CLICKHOUSE_CLIENT} --user "${USER}" -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_a_http UUID '$(attach_uuid a_http)' (a UInt32) ENGINE = URL('http://example.com/data.csv', 'CSV')" 2>&1)
rc=$?
if echo "$out" | grep -qiE "Not enough privileges|ACCESS_DENIED"; then
    echo "attach-http-DENIED (unexpected)"
elif [ "$rc" -eq 0 ] && attached "${CLICKHOUSE_TEST_UNIQUE_NAME}_a_http"; then
    echo "attach-http-allowed"
else
    echo "attach-http-FAILED (unexpected): rc=$rc $out"
fi
${CLICKHOUSE_CLIENT} --user "${USER}" -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_a_http"

# A short `ATTACH TABLE t` replays the definition already stored on this server, so it is not
# re-checked: revoking the target engine grant must not make an existing table unattachable.
echo "--- short ATTACH replays stored metadata and is not re-checked, even for a dispatched engine ---"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON File TO ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_reattach SYNC"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_reattach (s String) ENGINE = URL('file://${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_r.csv', 'LineAsString')"
${CLICKHOUSE_CLIENT} -q "REVOKE TABLE ENGINE ON File FROM ${USER}"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_reattach"
out=$(${CLICKHOUSE_CLIENT} --user "${USER}" -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_reattach" 2>&1)
rc=$?
if echo "$out" | grep -qiE "Not enough privileges|ACCESS_DENIED"; then
    echo "short-attach-DENIED (unexpected)"
elif [ "$rc" -eq 0 ] && attached "${CLICKHOUSE_TEST_UNIQUE_NAME}_reattach"; then
    echo "short-attach-allowed"
else
    echo "short-attach-FAILED (unexpected): rc=$rc $out"
fi
${CLICKHOUSE_CLIENT} --user "${USER}" -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_reattach"

# A `RESTORE` introduces a definition under whoever is restoring, who need not be the user the backup
# was taken from, so it is checked like a `CREATE` rather than treated as replayed metadata. The
# destination carries the shell's pid because a backup may not be written twice to the same one and
# the unique name alone is stable across repeated runs of one test.
BACKUP="Disk('backups', '04401_${CLICKHOUSE_TEST_UNIQUE_NAME}_$$')"
echo "--- RESTORE carries a fresh definition and is checked like CREATE: URL('file://...') needs the File grant ---"
# The backup must be taken from the definition written here, not from one a previous run left behind.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_restore SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_restore (s String) ENGINE = URL('file://${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_b.csv', 'LineAsString')"
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_restore TO ${BACKUP} FORMAT Null"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_restore SYNC"

${CLICKHOUSE_CLIENT} --user "${USER}" -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_restore FROM ${BACKUP} FORMAT Null" 2>&1 \
    | denied_on File && echo "restore-file-engine-denied" || echo "NOT DENIED"
attached "${CLICKHOUSE_TEST_UNIQUE_NAME}_restore" && echo "RESTORED ANYWAY (unexpected)" || echo "restore-rejected-not-created"

${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON File TO ${USER}"
out=$(${CLICKHOUSE_CLIENT} --user "${USER}" -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_restore FROM ${BACKUP} FORMAT Null" 2>&1)
rc=$?
if echo "$out" | grep -qiE "Not enough privileges|ACCESS_DENIED"; then
    echo "restore-with-grant-DENIED (unexpected)"
elif [ "$rc" -eq 0 ] && attached "${CLICKHOUSE_TEST_UNIQUE_NAME}_restore"; then
    echo "restore-with-grant-allowed"
else
    echo "restore-with-grant-FAILED (unexpected): rc=$rc $out"
fi
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TEST_UNIQUE_NAME}_restore SYNC"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"

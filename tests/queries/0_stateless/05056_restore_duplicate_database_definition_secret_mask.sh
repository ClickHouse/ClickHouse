#!/usr/bin/env bash
# Tags: no-fasttest, no-encrypted-storage
# Tag no-fasttest: requires the S3 endpoint
# Tag no-encrypted-storage: a backup from an encrypted disk restores only to an encrypted disk, so the restored Backup database gets no parts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

client_opts=(
    --allow_repeated_settings
    --send_logs_level 'error'
)

s1=${CLICKHOUSE_DATABASE}_s1
s2=${CLICKHOUSE_DATABASE}_s2
v1=${CLICKHOUSE_DATABASE}_v1
v2=${CLICKHOUSE_DATABASE}_v2
merged=${CLICKHOUSE_DATABASE}_merged
restored=${CLICKHOUSE_DATABASE}_restored

# Access key id 'test', secret 'testtest': the credentials the stateless suite uses for S3. They are
# two distinct strings, so the assertions below tell the id apart from the secret.
inner1="S3('http://localhost:11111/test/backups/${CLICKHOUSE_DATABASE}/dupdef1', 'test', 'testtest')"
inner2="S3('http://localhost:11111/test/backups/${CLICKHOUSE_DATABASE}/dupdef2', 'test', 'testtest')"
# The outer locator carries no credential, so a secret found in the restore error can only have come
# from one of the two archived definitions and not from the statement being executed.
outer="Disk('backups', '${CLICKHOUSE_DATABASE}_dupdef_outer')"

# The two tables have different names on purpose: two tables renamed into one target would reach the
# table-level duplicate-definition check first, and the database-level one is what is asserted here.
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -m -q "
DROP DATABASE IF EXISTS ${s1};
DROP DATABASE IF EXISTS ${s2};
DROP DATABASE IF EXISTS ${v1};
DROP DATABASE IF EXISTS ${v2};
DROP DATABASE IF EXISTS ${merged};
DROP DATABASE IF EXISTS ${restored};
CREATE DATABASE ${s1};
CREATE TABLE ${s1}.t1 (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${s1}.t1 SELECT number FROM numbers(3);
CREATE DATABASE ${s2};
CREATE TABLE ${s2}.t2 (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${s2}.t2 SELECT number FROM numbers(3);
BACKUP DATABASE ${s1} TO ${inner1} FORMAT Null;
BACKUP DATABASE ${s2} TO ${inner2} FORMAT Null;
CREATE DATABASE ${v1} ENGINE = Backup('${s1}', ${inner1});
CREATE DATABASE ${v2} ENGINE = Backup('${s2}', ${inner2});
BACKUP DATABASE ${v1}, DATABASE ${v2} TO ${outer} FORMAT Null;
"

# Both elements rename to one target, so the definition read second reaches the duplicate-definition
# check, which reports both archived definitions.
err=$(${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q \
    "RESTORE DATABASE ${v1} AS ${merged}, DATABASE ${v2} AS ${merged} FROM ${outer}" 2>&1)

# The first two counters pin the failure to that check rather than to an earlier one, so the
# remaining three cannot be satisfied by an unrelated error or by an empty result.
echo '-- database-level duplicate definition reached (must be 1)'
echo "$err" | grep -c -m1 'Extracted two different create queries for the same database'
echo '-- CANNOT_RESTORE_DATABASE (must be 1)'
echo "$err" | grep -c -m1 CANNOT_RESTORE_DATABASE
echo '-- secret occurrences in the error (must be 0)'
echo "$err" | grep -c testtest
echo '-- [HIDDEN] present in the error (must be 1)'
echo "$err" | grep -c -m1 '\[HIDDEN\]'
echo '-- archived locator still identifiable in the error (must be 1)'
echo "$err" | grep -c -m1 dupdef1

# The archived definition has to keep the credential as written: attaching a Backup database checks the
# backup with the credentials the locator carries, so a definition archived with '[HIDDEN]' would fail
# the restore below with ACCESS_DENIED instead of reading the source table through it.
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "RESTORE DATABASE ${v1} AS ${restored} FROM ${outer}" > /dev/null
echo '-- restored Backup database reads its source table (must be 3)'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "SELECT count() FROM ${restored}.t1"
echo '-- the restored locator is hidden on display (must be 1)'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q \
    "SHOW CREATE DATABASE ${restored} SETTINGS format_display_secrets_in_show_and_select = 0" | grep -c -m1 '\[HIDDEN\]'

${CLICKHOUSE_CLIENT} "${client_opts[@]}" -m -q "
DROP DATABASE IF EXISTS ${restored};
DROP DATABASE IF EXISTS ${merged};
DROP DATABASE ${v1};
DROP DATABASE ${v2};
DROP DATABASE ${s1};
DROP DATABASE ${s2};
"

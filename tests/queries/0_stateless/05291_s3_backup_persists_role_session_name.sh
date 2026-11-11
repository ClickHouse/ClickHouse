#!/usr/bin/env bash
# Tags: no-fasttest, no-encrypted-storage
# Tag no-fasttest: requires the S3 endpoint
# Tag no-encrypted-storage: a backup from an encrypted disk restores only to an encrypted disk, so the restored Backup database gets no parts.

# `role_session_name` is shown as [HIDDEN] wherever a definition is displayed. A backup must still
# archive the definition with the real value, because RESTORE recreates the object from the archived
# text: an S3 table or a Backup database restored with the literal '[HIDDEN]' would assume its role with
# the wrong session name. The archived definitions are read back from the backup itself, so the check
# does not go through the display layer that masks them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

client_opts=(
    --allow_repeated_settings
    --send_logs_level 'error'
)

src=${CLICKHOUSE_DATABASE}_src
view=${CLICKHOUSE_DATABASE}_view
view_restored=${CLICKHOUSE_DATABASE}_view_restored
tbl=${CLICKHOUSE_DATABASE}.t_rsn
tbl_restored=${CLICKHOUSE_DATABASE}.t_rsn_restored

inner_url="http://localhost:11111/test/backups/${CLICKHOUSE_DATABASE}/rsn_inner"
outer_url="http://localhost:11111/test/backups/${CLICKHOUSE_DATABASE}/rsn_outer"
# Access key id 'test', secret 'testtest': the credentials the stateless suite uses for S3.
inner="S3('${inner_url}', 'test', 'testtest')"
# The Backup database mounts the inner backup with the same key pair plus a role_session_name and no
# role_arn: nothing assumes a role, so no STS endpoint is needed, while the value still travels through
# every place a locator is archived.
inner_with_session="S3('${inner_url}', 'test', 'testtest', extra_credentials(role_session_name = 'SEKRIT_DBRSN'))"
outer="S3('${outer_url}', 'test', 'testtest')"

# The S3 table carries the full assume-role triple. Nothing reads it here, so no STS endpoint is needed
# either: the table function connects lazily, on the first read.
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -m -q "
DROP DATABASE IF EXISTS ${src};
DROP DATABASE IF EXISTS ${view};
DROP DATABASE IF EXISTS ${view_restored};
DROP TABLE IF EXISTS ${tbl};
DROP TABLE IF EXISTS ${tbl_restored};
CREATE DATABASE ${src};
CREATE TABLE ${src}.t (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${src}.t SELECT number FROM numbers(10);
BACKUP DATABASE ${src} TO ${inner} FORMAT Null;
CREATE DATABASE ${view} ENGINE = Backup('${src}', ${inner_with_session});
CREATE TABLE ${tbl} (id UInt64) ENGINE = S3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/t_rsn', 'ak', 'sk',
    extra_credentials(role_arn = 'arn::role', role_session_name = 'SEKRIT_TRSN', external_id = 'SEKRIT_TEID'), format = 'CSV');
BACKUP DATABASE ${view}, TABLE ${tbl} TO ${outer} FORMAT Null;
"

# Reads one archived definition out of the outer backup, bypassing every display surface.
function archived()
{
    local path_in_backup=$1 && shift
    ${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q \
        "SELECT line FROM s3('${outer_url}/${path_in_backup}', 'test', 'testtest', 'LineAsString') FORMAT TSVRaw"
}

echo '-- archived Backup database definition keeps role_session_name (must be 1)'
archived "metadata/${view}.sql" | grep -c SEKRIT_DBRSN || true
echo '-- archived Backup database definition contains no [HIDDEN] (must be 0)'
archived "metadata/${view}.sql" | grep -c HIDDEN || true
echo '-- archived S3 table definition keeps role_session_name (must be 1)'
archived "metadata/${CLICKHOUSE_DATABASE}/t_rsn.sql" | grep -c SEKRIT_TRSN || true
echo '-- archived S3 table definition keeps external_id (must be 1)'
archived "metadata/${CLICKHOUSE_DATABASE}/t_rsn.sql" | grep -c SEKRIT_TEID || true
echo '-- archived S3 table definition contains no [HIDDEN] (must be 0)'
archived "metadata/${CLICKHOUSE_DATABASE}/t_rsn.sql" | grep -c HIDDEN || true

# The round trip: both objects come back from the archived text. The restored Backup database mounts
# the inner backup through its archived locator, so it is readable only if that locator was archived
# as written.
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q \
    "RESTORE DATABASE ${view} AS ${view_restored}, TABLE ${tbl} AS ${tbl_restored} FROM ${outer} FORMAT Null"

echo '-- restored Backup database reads its source table (must be 10)'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "SELECT count() FROM ${view_restored}.t"

# On display the restored definitions are masked like any other: the archived value never shows.
echo '-- secret occurrences in SHOW CREATE DATABASE of the restored Backup database (must be 0)'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "SHOW CREATE DATABASE ${view_restored}" | grep -c SEKRIT || true
echo '-- [HIDDEN] present in SHOW CREATE DATABASE of the restored Backup database (must be 1)'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "SHOW CREATE DATABASE ${view_restored}" | grep -c -m1 '\[HIDDEN\]'
echo '-- secret occurrences in SHOW CREATE TABLE of the restored S3 table (must be 0)'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "SHOW CREATE TABLE ${tbl_restored}" | grep -c SEKRIT || true
echo '-- [HIDDEN] present in SHOW CREATE TABLE of the restored S3 table (must be 1)'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "SHOW CREATE TABLE ${tbl_restored}" | grep -c -m1 '\[HIDDEN\]'

${CLICKHOUSE_CLIENT} "${client_opts[@]}" -m -q "
DROP TABLE ${tbl_restored};
DROP TABLE ${tbl};
DROP DATABASE ${view_restored};
DROP DATABASE ${view};
DROP DATABASE ${src};
"

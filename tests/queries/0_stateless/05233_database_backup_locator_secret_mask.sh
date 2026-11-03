#!/usr/bin/env bash
# Tags: no-fasttest, no-encrypted-storage
# Tag no-fasttest: requires S3
# Tag no-encrypted-storage: a backup from an encrypted disk restores only to an encrypted disk, so the Backup database gets no parts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

client_opts=(
    --allow_repeated_settings
    --send_logs_level 'error'
)

db=${CLICKHOUSE_DATABASE}_src
view=${CLICKHOUSE_DATABASE}_view
# The credentials the stateless suite uses for S3 backups: access key id 'test', secret 'testtest'.
# They are two distinct strings, so the assertions below tell the id apart from the secret.
dest="S3('http://localhost:11111/test/backups/${CLICKHOUSE_DATABASE}/locator_mask', 'test', 'testtest')"

# BACKUP TO S3 emits a bandwidth warning on stderr under some configurations, which would break
# the reference in a bare run, so keep the log level at error throughout (as 02843 and 04327 do).
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -m -q "
DROP DATABASE IF EXISTS ${db};
DROP DATABASE IF EXISTS ${view};
CREATE DATABASE ${db};
CREATE TABLE ${db}.t (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${db}.t SELECT number FROM numbers(10);
BACKUP DATABASE ${db} TO ${dest} FORMAT Null;
CREATE DATABASE ${view} ENGINE = Backup('${db}', ${dest});
"

# Both display surfaces of a real, successfully created Backup database. The locator must stay a
# nested S3 function so that only its secret_access_key is replaced by [HIDDEN]: the url and the
# access key id must remain visible, and the two surfaces must agree.
echo '-- SHOW CREATE DATABASE'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "SHOW CREATE DATABASE ${view}"
echo '-- system.databases.engine_full'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "SELECT engine_full FROM system.databases WHERE name = '${view}'"

# Explicit counters, so a regression is visible on its own line rather than buried in a long one.
echo '-- secret occurrences in SHOW CREATE (must be 0)'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "SHOW CREATE DATABASE ${view}" | grep -c testtest
echo '-- [HIDDEN] present in SHOW CREATE (must be 1)'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "SHOW CREATE DATABASE ${view}" | grep -c -m1 '\[HIDDEN\]'
echo '-- secret occurrences in system.databases.engine_full (must be 0)'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "SELECT engine_full FROM system.databases WHERE name = '${view}'" | grep -c testtest
echo '-- [HIDDEN] present in system.databases.engine_full (must be 1)'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "SELECT engine_full FROM system.databases WHERE name = '${view}'" | grep -c -m1 '\[HIDDEN\]'

# The Backup database is readable, so the masked surfaces above are not the output of a broken database.
echo '-- rows visible through the Backup database'
${CLICKHOUSE_CLIENT} "${client_opts[@]}" -q "SELECT count() FROM ${view}.t"

${CLICKHOUSE_CLIENT} "${client_opts[@]}" -m -q "
DROP DATABASE ${view};
DROP DATABASE ${db};
"

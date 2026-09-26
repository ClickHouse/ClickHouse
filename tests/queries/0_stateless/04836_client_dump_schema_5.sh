#!/usr/bin/env bash
# Tags: no-darwin, no-encrypted-storage

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SOURCE_DB="${CLICKHOUSE_DATABASE}_backup_source"
BACKUP_DB="${CLICKHOUSE_DATABASE}_backup_engine"
BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}"
ERR_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_backup.err"

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${BACKUP_DB};
    DROP DATABASE IF EXISTS ${SOURCE_DB};
    CREATE DATABASE ${SOURCE_DB};
    CREATE TABLE ${SOURCE_DB}.t (id UInt64) ENGINE = MergeTree ORDER BY id;
    BACKUP DATABASE ${SOURCE_DB} TO Disk('backups', '${BACKUP_NAME}') FORMAT Null;
    CREATE DATABASE ${BACKUP_DB} ENGINE = Backup('${SOURCE_DB}', Disk('backups', '${BACKUP_NAME}'));
"

echo '--- a Backup database is rejected instead of emitting an unreplayable CREATE ---'
if $CLICKHOUSE_CLIENT --dump-schema="${BACKUP_DB}" > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: Backup database dump succeeded'
else
    echo "Backup database refusal is explicit: $(grep -c 'Backup engine is not replayable' "$ERR_FILE")"
fi

$CLICKHOUSE_CLIENT --multiquery --query "DROP DATABASE IF EXISTS ${BACKUP_DB}; DROP DATABASE IF EXISTS ${SOURCE_DB} SYNC;"
rm -f "$ERR_FILE"

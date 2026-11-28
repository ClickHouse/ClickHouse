#!/usr/bin/env bash
# Tags: no-encrypted-storage

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP_DATABASE_NAME=${CLICKHOUSE_TEST_UNIQUE_NAME}_backup
ATTACHED_DATABASE_NAME=${CLICKHOUSE_TEST_UNIQUE_NAME}_attached

$CLICKHOUSE_CLIENT -q """
DROP DATABASE IF EXISTS $BACKUP_DATABASE_NAME;
CREATE DATABASE $BACKUP_DATABASE_NAME;

CREATE TABLE $BACKUP_DATABASE_NAME.test_table (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO $BACKUP_DATABASE_NAME.test_table SELECT number FROM numbers(10);

BACKUP DATABASE $BACKUP_DATABASE_NAME TO Disk('backups', '$BACKUP_DATABASE_NAME') FORMAT Null;

DROP DATABASE IF EXISTS $ATTACHED_DATABASE_NAME;
CREATE DATABASE $ATTACHED_DATABASE_NAME ENGINE = Backup('$BACKUP_DATABASE_NAME', Disk('backups', '$BACKUP_DATABASE_NAME'));

SELECT count() FROM $ATTACHED_DATABASE_NAME.test_table;
"""

# A comment change rewrites the metadata file of the database, so the locator has to survive the
# round trip: loading it back is what the server does at every start.
$CLICKHOUSE_CLIENT -q """
ALTER DATABASE $ATTACHED_DATABASE_NAME MODIFY COMMENT 'a comment';
DETACH DATABASE $ATTACHED_DATABASE_NAME;
ATTACH DATABASE $ATTACHED_DATABASE_NAME;

SELECT count() FROM $ATTACHED_DATABASE_NAME.test_table;
SELECT comment FROM system.databases WHERE name = '$ATTACHED_DATABASE_NAME';
"""

# A locator serialized as a string literal is accepted only while loading metadata an older server
# rewrote: in a statement a user writes it must be the function it is, because that is the form the
# secret masker redacts.
$CLICKHOUSE_CLIENT -q """
DROP DATABASE $ATTACHED_DATABASE_NAME;
CREATE DATABASE $ATTACHED_DATABASE_NAME ENGINE = Backup('$BACKUP_DATABASE_NAME', 'Disk(\\'backups\\', \\'$BACKUP_DATABASE_NAME\\')');
""" 2>&1 | grep -q -F 'Expected function' && echo 'refused'

$CLICKHOUSE_CLIENT -q """
DROP DATABASE IF EXISTS $ATTACHED_DATABASE_NAME;
DROP DATABASE $BACKUP_DATABASE_NAME;
"""

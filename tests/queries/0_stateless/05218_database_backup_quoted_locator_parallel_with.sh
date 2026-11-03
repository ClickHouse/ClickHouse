#!/usr/bin/env bash
# Tags: no-encrypted-storage

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP_DATABASE_NAME=${CLICKHOUSE_TEST_UNIQUE_NAME}_backup
ATTACHED_DATABASE_NAME=${CLICKHOUSE_TEST_UNIQUE_NAME}_attached
OTHER_DATABASE_NAME=${CLICKHOUSE_TEST_UNIQUE_NAME}_other

$CLICKHOUSE_CLIENT -q """
DROP DATABASE IF EXISTS $BACKUP_DATABASE_NAME;
DROP DATABASE IF EXISTS $ATTACHED_DATABASE_NAME;
DROP DATABASE IF EXISTS $OTHER_DATABASE_NAME;
CREATE DATABASE $BACKUP_DATABASE_NAME;

CREATE TABLE $BACKUP_DATABASE_NAME.test_table (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO $BACKUP_DATABASE_NAME.test_table SELECT number FROM numbers(10);

BACKUP DATABASE $BACKUP_DATABASE_NAME TO Disk('backups', '$BACKUP_DATABASE_NAME') FORMAT Null;
"""

# `PARALLEL WITH` runs its statements as internal queries, and the full `ATTACH DATABASE ... ENGINE = ...`
# form runs in `ATTACH` mode - the same pair the server's own replay of stored metadata shows. The quoted
# locator is accepted only on that replay, never in a statement a user writes, however it is wrapped.
$CLICKHOUSE_CLIENT -q """
CREATE DATABASE $OTHER_DATABASE_NAME
PARALLEL WITH
ATTACH DATABASE $ATTACHED_DATABASE_NAME ENGINE = Backup('$BACKUP_DATABASE_NAME', 'Disk(\\'backups\\', \\'$BACKUP_DATABASE_NAME\\')');
""" 2>&1 | grep -q -F 'Expected function' && echo 'refused'

# Each block below must start from the same state: whether the sibling `CREATE DATABASE` of a
# `PARALLEL WITH` whose other statement threw is committed or rolled back depends on how the
# statements were scheduled, and with `max_threads = 1` it is committed. Drop it in between, so the
# refusal under test is what the block observes, not a leftover "database already exists".
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $OTHER_DATABASE_NAME"

$CLICKHOUSE_CLIENT -q """
CREATE DATABASE $OTHER_DATABASE_NAME
PARALLEL WITH
CREATE DATABASE $ATTACHED_DATABASE_NAME ENGINE = Backup('$BACKUP_DATABASE_NAME', 'Disk(\\'backups\\', \\'$BACKUP_DATABASE_NAME\\')');
""" 2>&1 | grep -q -F 'Expected function' && echo 'refused'

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $OTHER_DATABASE_NAME"

# A quoted locator can carry credentials, and `PARALLEL WITH` formats its statements before the engine
# refuses them, so the formatted text must hide it: neither the logged query text (the same text an
# `ON CLUSTER` statement puts into the distributed DDL payload) nor the refusal message may carry it.
$CLICKHOUSE_CLIENT -q """
CREATE DATABASE $OTHER_DATABASE_NAME
PARALLEL WITH
ATTACH DATABASE $ATTACHED_DATABASE_NAME ENGINE = Backup('$BACKUP_DATABASE_NAME', 'S3(\\'http://localhost:11111/05218\\', \\'ak\\', \\'SEKRIT_05218\\')');
""" 2>&1 | grep -q -F 'Expected function' && echo 'refused'

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $OTHER_DATABASE_NAME"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q """
SELECT countIf(query LIKE '%SEKRIT_05218%' OR exception LIKE '%SEKRIT_05218%'), countIf(query LIKE '%Backup(%[HIDDEN]%') > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE;
"""

# The function form goes through the same wrapper.
$CLICKHOUSE_CLIENT -q """
CREATE DATABASE IF NOT EXISTS $OTHER_DATABASE_NAME
PARALLEL WITH
ATTACH DATABASE $ATTACHED_DATABASE_NAME ENGINE = Backup('$BACKUP_DATABASE_NAME', Disk('backups', '$BACKUP_DATABASE_NAME'));

SELECT count() FROM $ATTACHED_DATABASE_NAME.test_table;
"""

$CLICKHOUSE_CLIENT -q """
DROP DATABASE IF EXISTS $ATTACHED_DATABASE_NAME;
DROP DATABASE IF EXISTS $OTHER_DATABASE_NAME;
DROP DATABASE $BACKUP_DATABASE_NAME;
"""

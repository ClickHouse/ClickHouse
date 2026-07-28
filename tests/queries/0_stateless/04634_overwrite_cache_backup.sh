#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `BACKUP` copies the immutable segment files and a self-contained manifest, and a `RESTORE` replays
# them, so the restored table has to answer both a primary and a lookup-index query.

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS overwrite_cache_backup;

CREATE TABLE overwrite_cache_backup
(
    website_type UInt8,
    user_id UInt64,
    tag LowCardinality(String),
    version UInt64,
    value String
)
ENGINE = OverwriteCache(version)
KEYS (website_type, user_id, tag)
INDEX (tag);

INSERT INTO overwrite_cache_backup VALUES (1, 42, 'risk', 1, 'first');
INSERT INTO overwrite_cache_backup VALUES (1, 43, 'risk', 1, 'second');
INSERT INTO overwrite_cache_backup VALUES (1, 42, 'risk', 2, 'replaced');
DELETE FROM overwrite_cache_backup WHERE website_type = 1 AND user_id = 43 AND tag = 'risk';
"

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE overwrite_cache_backup TO ${backup_name} FORMAT Null"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE overwrite_cache_backup;
RESTORE TABLE overwrite_cache_backup FROM ${backup_name} FORMAT Null;
"

${CLICKHOUSE_CLIENT} -m --query "
SELECT 'restored primary lookup', value FROM overwrite_cache_backup WHERE website_type = 1 AND user_id = 42 AND tag = 'risk';
SELECT 'restored index lookup', count() FROM overwrite_cache_backup WHERE tag = 'risk';
SELECT 'deleted key stays deleted', count() FROM overwrite_cache_backup WHERE website_type = 1 AND user_id = 43 AND tag = 'risk';
"

# Restoring over a table that already holds rows would merge two unrelated caches, so it is refused.
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE overwrite_cache_backup FROM ${backup_name} FORMAT Null" 2>&1 \
    | grep -c -m1 "CANNOT_RESTORE_TABLE"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE overwrite_cache_backup;

-- A table that keeps nothing on disk has nothing to back up, which is an error rather than an empty backup.
CREATE TABLE overwrite_cache_volatile
(
    key UInt64,
    version UInt64
)
ENGINE = OverwriteCache(version)
KEYS (key)
SETTINGS persist_mode = 'none';
INSERT INTO overwrite_cache_volatile VALUES (1, 1);
"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE overwrite_cache_volatile TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_volatile') FORMAT Null" 2>&1 \
    | grep -c -m1 "BAD_ARGUMENTS"

${CLICKHOUSE_CLIENT} --query "DROP TABLE overwrite_cache_volatile"

#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
CREATE TABLE test_plain_rewr_ts_04103 (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(type = 'object_storage', object_storage_type = 'local', path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}/', metadata_type = 'plain_rewritable');

INSERT INTO test_plain_rewr_ts_04103 VALUES (1, 'hello'), (2, 'world');

SELECT
    toYear(column_modification_time) >= 2025 AS year_ok,
    abs(toUnixTimestamp(now()) - toUnixTimestamp(column_modification_time)) < 600 AS is_recent
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_plain_rewr_ts_04103' AND active AND column = 'a'
ORDER BY name;
"

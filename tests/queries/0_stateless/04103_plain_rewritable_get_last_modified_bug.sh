#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A shell test rather than a `.sql` one only because the disk names a directory, and a disk defined
# in SQL may only name one inside `custom_local_disks_base_directory`, whose location the test can
# learn only from the environment.

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE test_plain_rewr_ts_04103 (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(type = 'object_storage', object_storage_type = 'local', path = '${CLICKHOUSE_DISKS_FILES}/plain_rewritable_04103_${CLICKHOUSE_DATABASE}/', metadata_type = 'plain_rewritable');

INSERT INTO test_plain_rewr_ts_04103 VALUES (1, 'hello'), (2, 'world');

SELECT
    toYear(column_modification_time) >= 2025 AS year_ok,
    abs(toUnixTimestamp(now()) - toUnixTimestamp(column_modification_time)) < 600 AS is_recent
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_plain_rewr_ts_04103' AND active AND column = 'a'
ORDER BY name;
"

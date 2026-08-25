#!/usr/bin/env bash
# Tags: no-fasttest, no-encrypted-storage, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t0 SYNC;
DROP TABLE IF EXISTS t1 SYNC;

CREATE TABLE t0 (c1 Int) ENGINE = MergeTree() ORDER BY c1 PARTITION BY (c1 % 10);
INSERT INTO TABLE t0 (c1) SELECT number FROM numbers(500);

BACKUP TABLE t0 TO Disk('backups', '03760_backup_tar_archive_$CLICKHOUSE_TEST_UNIQUE_NAME.tar') FORMAT Null;

RESTORE TABLE t0 AS t1 FROM Disk('backups', '03760_backup_tar_archive_$CLICKHOUSE_TEST_UNIQUE_NAME.tar') FORMAT Null;

SELECT * FROM t1 ORDER BY c1 LIMIT 10;
"

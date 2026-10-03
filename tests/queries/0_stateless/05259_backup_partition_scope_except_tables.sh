#!/usr/bin/env bash
# BACKUP TABLE t PARTITION ..., DATABASE db EXCEPT TABLES t backs up only the named partition of t

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS t;
CREATE TABLE t (part UInt8, id UInt64) ENGINE = MergeTree PARTITION BY part ORDER BY id;
INSERT INTO t VALUES (1, 1), (1, 2), (2, 3);

BACKUP TABLE t PARTITION 1, DATABASE ${CLICKHOUSE_DATABASE} EXCEPT TABLES t TO Memory('except_tables') FORMAT Null;
BACKUP TABLE t PARTITION 1, DATABASE ${CLICKHOUSE_DATABASE} TO Memory('whole_database') FORMAT Null;

DROP TABLE t SYNC;
RESTORE TABLE t FROM Memory('except_tables') FORMAT Null;
SELECT 'except_tables', part, count() FROM t GROUP BY part ORDER BY part;

DROP TABLE t SYNC;
RESTORE TABLE t FROM Memory('whole_database') FORMAT Null;
SELECT 'whole_database', part, count() FROM t GROUP BY part ORDER BY part;

DROP TABLE t;
"

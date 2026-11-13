#!/bin/bash
# Tags: log-engine
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATABASE_ATOMIC="${CLICKHOUSE_DATABASE}_atomic"
DATABASE_LAZY="${CLICKHOUSE_DATABASE}_lazy"

$CLICKHOUSE_CLIENT "

SELECT 'database atomic tests';
DROP DATABASE IF EXISTS ${DATABASE_ATOMIC};
CREATE DATABASE IF NOT EXISTS ${DATABASE_ATOMIC} ENGINE=Atomic;

CREATE TABLE ${DATABASE_ATOMIC}.test_table (n Int64) ENGINE=MergeTree ORDER BY n;
SELECT * FROM system.detached_tables WHERE database='${DATABASE_ATOMIC}';

DETACH TABLE ${DATABASE_ATOMIC}.test_table;
SELECT database, table, is_permanently FROM system.detached_tables WHERE database='${DATABASE_ATOMIC}';

ATTACH TABLE ${DATABASE_ATOMIC}.test_table;

CREATE TABLE ${DATABASE_ATOMIC}.test_table_perm (n Int64) ENGINE=MergeTree ORDER BY n;
SELECT * FROM system.detached_tables WHERE database='${DATABASE_ATOMIC}';

DETACH TABLE ${DATABASE_ATOMIC}.test_table_perm PERMANENTLY;
SELECT database, table, is_permanently FROM system.detached_tables WHERE database='${DATABASE_ATOMIC}';

DETACH TABLE ${DATABASE_ATOMIC}.test_table SYNC;
SELECT database, table, is_permanently FROM system.detached_tables WHERE database='${DATABASE_ATOMIC}';

SELECT database, table, is_permanently FROM system.detached_tables WHERE database='${DATABASE_ATOMIC}' AND table='test_table';

DROP DATABASE ${DATABASE_ATOMIC} SYNC;

"

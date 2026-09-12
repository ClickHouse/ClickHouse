#!/usr/bin/env bash
# An explicit BACKUP TABLE through a read-only Overlay facade is rejected with CANNOT_BACKUP_TABLE
# (the facade owns no tables), instead of misreporting the table as missing with UNKNOWN_TABLE.
# The rejection must not depend on whether the table exists, so it cannot be used as an existence oracle.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_SRC="${CLICKHOUSE_DATABASE}_src"
DB_OVL="${CLICKHOUSE_DATABASE}_ovl"

${CLICKHOUSE_CLIENT} --query "
DROP DATABASE IF EXISTS ${DB_OVL};
DROP DATABASE IF EXISTS ${DB_SRC};
CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
CREATE TABLE ${DB_SRC}.t (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${DB_SRC}.t VALUES (1), (2), (3);
CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');
"

echo "backup table through the facade (existing table)"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${DB_OVL}.t TO Memory('${CLICKHOUSE_TEST_UNIQUE_NAME}_t') FORMAT Null" 2>&1 \
    | grep -o -m1 'CANNOT_BACKUP_TABLE' || echo "unexpected result"

echo "backup table through the facade (nonexistent table): same error"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${DB_OVL}.no_such_table TO Memory('${CLICKHOUSE_TEST_UNIQUE_NAME}_m') FORMAT Null" 2>&1 \
    | grep -o -m1 'CANNOT_BACKUP_TABLE' || echo "unexpected result"

echo "backup table from the owning database works"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${DB_SRC}.t TO Memory('${CLICKHOUSE_TEST_UNIQUE_NAME}_src') FORMAT Null" && echo "OK"

echo "backup database of the facade still works"
${CLICKHOUSE_CLIENT} --query "BACKUP DATABASE ${DB_OVL} TO Memory('${CLICKHOUSE_TEST_UNIQUE_NAME}_db') FORMAT Null" && echo "OK"

${CLICKHOUSE_CLIENT} --query "
DROP DATABASE ${DB_OVL};
DROP DATABASE ${DB_SRC};
"

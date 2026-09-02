#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag justification:
#   no-fasttest: depends on libpq (PostgreSQL database engine), which is not built in fast test.
#   no-parallel: creates a PostgreSQL database pointing at an unreachable endpoint. Because
#     `show_remote_databases_in_system_tables` defaults to `true`, the database is visible in
#     `system.tables` and `system.columns`, so any concurrent query that scans those tables
#     without a database filter would try to connect to the unreachable endpoint and fail.
#
# `ALTER TABLE` and `UPDATE` through a read-only `Overlay` facade must be rejected up front by the
# database name, before the interpreter's eager table lookup. `InterpreterAlterQuery` and the
# `_row_exists` prepass of `InterpreterUpdateQuery` used to call `tryGetTable` first, which loads
# the underlying source table - so for a source backed by an unavailable remote catalog the
# source's own connection error surfaced through the facade before the read-only rejection ran,
# turning the facade into an oracle for hidden or broken sources. All rejections must answer
# `TABLE_IS_PERMANENTLY_READ_ONLY`, indistinguishably for existing, missing, and unreachable
# source tables, and `ALTER`/`UPDATE` in the underlying database itself must keep working.
# Related: https://github.com/ClickHouse/ClickHouse/pull/86768

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_PG="db_pg_${SUF}"
DB_OVL="db_ovl_${SUF}"
DB_OVL_PG="db_ovl_pg_${SUF}"

# The connection errors that the probes produce are logged server-side at error level; keep them
# out of the test's stderr.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --send_logs_level=fatal"

# The PostgreSQL database engine does not connect at CREATE time, so an unreachable endpoint is
# fine here. Port 1 on localhost is never listening, so every probe fails instantly with
# "connection refused" instead of hanging.
${CLICKHOUSE_CLIENT} -m -q "
DROP DATABASE IF EXISTS ${DB_OVL};
DROP DATABASE IF EXISTS ${DB_OVL_PG};
DROP DATABASE IF EXISTS ${DB_SRC};
DROP DATABASE IF EXISTS ${DB_PG};

CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
CREATE TABLE ${DB_SRC}.t (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO ${DB_SRC}.t VALUES (1, 10), (2, 20);

CREATE DATABASE ${DB_PG} ENGINE = PostgreSQL('127.0.0.1:1', 'fake_db', 'user', 'password');

CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');
CREATE DATABASE ${DB_OVL_PG} ENGINE = Overlay('${DB_PG}');
"

echo "-- ALTER of an existing source table through the facade: rejected"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${DB_OVL}.t DROP COLUMN val" 2>&1 | grep -oF 'TABLE_IS_PERMANENTLY_READ_ONLY' | head -1

echo "-- ALTER of a missing source table through the facade: the same error (no existence oracle)"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${DB_OVL}.no_such_table DROP COLUMN val" 2>&1 | grep -oF 'TABLE_IS_PERMANENTLY_READ_ONLY' | head -1

echo "-- ALTER through a facade over an unreachable source: still the same error, not the connection error"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${DB_OVL_PG}.t DROP COLUMN val" 2>&1 | grep -oF 'TABLE_IS_PERMANENTLY_READ_ONLY' | head -1

echo "-- UPDATE of an existing source table through the facade: rejected"
${CLICKHOUSE_CLIENT} --enable_lightweight_update=1 -q "UPDATE ${DB_OVL}.t SET val = 0 WHERE 1" 2>&1 | grep -oF 'TABLE_IS_PERMANENTLY_READ_ONLY' | head -1

echo "-- UPDATE of a missing source table through the facade: the same error (no existence oracle)"
${CLICKHOUSE_CLIENT} --enable_lightweight_update=1 -q "UPDATE ${DB_OVL}.no_such_table SET val = 0 WHERE 1" 2>&1 | grep -oF 'TABLE_IS_PERMANENTLY_READ_ONLY' | head -1

echo "-- UPDATE through a facade over an unreachable source: still the same error, not the connection error"
${CLICKHOUSE_CLIENT} --enable_lightweight_update=1 -q "UPDATE ${DB_OVL_PG}.t SET val = 0 WHERE 1" 2>&1 | grep -oF 'TABLE_IS_PERMANENTLY_READ_ONLY' | head -1

echo "-- ALTER and UPDATE in the underlying database keep working"
${CLICKHOUSE_CLIENT} --enable_lightweight_update=1 -m -q "
ALTER TABLE ${DB_SRC}.t ADD COLUMN extra UInt64 DEFAULT 7;
UPDATE ${DB_SRC}.t SET val = val + 1 WHERE id = 1;
SELECT id, val, extra FROM ${DB_SRC}.t ORDER BY id;
"

${CLICKHOUSE_CLIENT} -m -q "
DROP DATABASE ${DB_OVL};
DROP DATABASE ${DB_OVL_PG};
DROP DATABASE ${DB_SRC};
DROP DATABASE ${DB_PG};
"

#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# Tag no-parallel: uses fail points which affect the whole server.
# Tag no-replicated-database: exercises the plain MergeTree durable metadata path
# and uses DETACH/ATTACH (rejected under Replicated) to reload mutations from disk.
#
# Regression for https://github.com/ClickHouse/ClickHouse/issues/113615:
# TTL-only ALTER MODIFY TTL must commit durable metadata before writing
# mutation_*.txt. A crash after metadata commit must not leave a MATERIALIZE TTL
# mutation that retries forever against metadata without TTL and wedges later
# mutations (e.g. UPDATE).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

echo "=== happy path: MODIFY TTL still works ==="

$CLICKHOUSE_CLIENT --query="
    DROP TABLE IF EXISTS t_ttl_alter_ok;

    CREATE TABLE t_ttl_alter_ok (id UInt64, d DateTime)
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;

    INSERT INTO t_ttl_alter_ok VALUES (1, now());

    ALTER TABLE t_ttl_alter_ok MODIFY TTL d + INTERVAL 1 MONTH SETTINGS materialize_ttl_after_modify = 1, alter_sync = 2;
"

$CLICKHOUSE_CLIENT --query="
    SELECT create_table_query LIKE '%TTL%'
    FROM system.tables
    WHERE database = currentDatabase() AND name = 't_ttl_alter_ok';
"

$CLICKHOUSE_CLIENT --query="
    SELECT count()
    FROM system.mutations
    WHERE database = currentDatabase()
      AND table = 't_ttl_alter_ok'
      AND is_done = 0
      AND latest_fail_reason LIKE '%Cannot MATERIALIZE TTL%';
"

$CLICKHOUSE_CLIENT --query="DROP TABLE t_ttl_alter_ok"

echo "=== crash after TTL metadata commit: no mutation wedge ==="

$CLICKHOUSE_CLIENT --query="
    DROP TABLE IF EXISTS t_ttl_alter_crash;

    CREATE TABLE t_ttl_alter_crash (id UInt64, d DateTime, v UInt64)
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;

    INSERT INTO t_ttl_alter_crash VALUES (1, now(), 10);

    SYSTEM ENABLE FAILPOINT mt_alter_throw_after_ttl_metadata_commit;
"

set +e
$CLICKHOUSE_CLIENT --query="ALTER TABLE t_ttl_alter_crash MODIFY TTL d + INTERVAL 1 MONTH SETTINGS materialize_ttl_after_modify = 1, alter_sync = 2" 2>/dev/null
alter_status=$?
set -e

$CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT mt_alter_throw_after_ttl_metadata_commit"

if [ "$alter_status" -eq 0 ]; then
    echo "FAIL: ALTER unexpectedly succeeded; failpoint did not fire"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_ttl_alter_crash"
    exit 1
fi

# Simulate restart: reload table metadata and mutations from disk.
$CLICKHOUSE_CLIENT --query="DETACH TABLE t_ttl_alter_crash"
$CLICKHOUSE_CLIENT --query="ATTACH TABLE t_ttl_alter_crash"

# Durable metadata kept the new TTL (metadata-first path).
$CLICKHOUSE_CLIENT --query="
    SELECT create_table_query LIKE '%TTL%'
    FROM system.tables
    WHERE database = currentDatabase() AND name = 't_ttl_alter_crash';
"

# Must not be wedged on an impossible MATERIALIZE TTL mutation.
$CLICKHOUSE_CLIENT --query="
    SELECT count()
    FROM system.mutations
    WHERE database = currentDatabase()
      AND table = 't_ttl_alter_crash'
      AND latest_fail_reason LIKE '%Cannot MATERIALIZE TTL%';
"

# Later mutations must still be able to proceed.
$CLICKHOUSE_CLIENT --query="
    ALTER TABLE t_ttl_alter_crash UPDATE v = 20 WHERE id = 1 SETTINGS mutations_sync = 2;
    SELECT v FROM t_ttl_alter_crash WHERE id = 1;
"

$CLICKHOUSE_CLIENT --query="DROP TABLE t_ttl_alter_crash"

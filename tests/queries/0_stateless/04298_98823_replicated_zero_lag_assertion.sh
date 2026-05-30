#!/usr/bin/env bash
# Tags: zookeeper

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/98823.
#
# Setting `max_replication_lag_to_enqueue` to `0` made the post-recovery
# unsynced check
#     max_log_ptr + max_replication_lag_to_enqueue <= new_max_log_ptr
# trivially true (the ZooKeeper counter is monotonically non-decreasing),
# so a fully caught-up replica was mis-marked as unsynced and the next DDL
# tripped `chassert(our_log_ptr < max_log_ptr)` in `initAndCheckTask`,
# aborting the server in debug and sanitizer builds.
#
# `0` has no sensible semantics for this setting (it asks for zero lag
# tolerance, which is impossible to satisfy under concurrent commits), so
# the fix forbids it at parse time. Switching the setting type to
# `NonZeroUInt64` makes the parser reject `0` everywhere the value can be
# supplied (`CREATE DATABASE ... SETTINGS`, `ALTER DATABASE`, and the
# `<database_replicated>` server-config block), in `ATTACH` replay, and on
# upgrade from older versions whose metadata persists `0`. `1` is the
# smallest meaningful value: it means "consider the replica unsynced as
# soon as there is at least one new entry to apply".

# Keep server logs forwarded to client stderr quiet so the rejection error
# is the only content in the captured stream. Must be set before sourcing
# shell_config.sh so that `--send_logs_level=fatal` ends up in the canonical
# CLICKHOUSE_CLIENT_OPT (avoids `option ... cannot be specified more than
# once` when the test also passes the flag inline).
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db_zero="rdb_zero_lag_${CLICKHOUSE_DATABASE}_zero"
db_one="rdb_zero_lag_${CLICKHOUSE_DATABASE}_one"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db_zero} SYNC"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db_one} SYNC"

# 1. `max_replication_lag_to_enqueue = 0` must be rejected with BAD_ARGUMENTS.
if ${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${db_zero}
    ENGINE = Replicated('/test/${CLICKHOUSE_DATABASE}/rdb_zero_lag_zero', 's1', 'r1')
    SETTINGS max_replication_lag_to_enqueue = 0
" 2>&1 | grep -q -F "A setting's value has to be greater than 0"
then
    echo "rejected"
else
    echo "NOT rejected (bug)"
fi

# 2. The database must not have been created.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = '${db_zero}'"

# 3. The smallest valid value (1) still works and the post-recovery DDL
# path stays alive, which is what was crashing on the original report.
${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${db_one}
    ENGINE = Replicated('/test/${CLICKHOUSE_DATABASE}/rdb_zero_lag_one', 's1', 'r1')
    SETTINGS max_replication_lag_to_enqueue = 1
"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "CREATE TABLE ${db_one}.t (n Int) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${db_one}.t VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${db_one}.t"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db_zero} SYNC"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db_one} SYNC"

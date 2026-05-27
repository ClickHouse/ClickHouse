#!/usr/bin/env bash
# Tags: zookeeper, no-parallel

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/98823.
#
# With `max_replication_lag_to_enqueue` set to `0`, `initializeReplication`
# used the condition `max_log_ptr + 0 <= new_max_log_ptr` (always true,
# because `max_log_ptr` is monotonically non-decreasing in ZooKeeper) to
# decide whether the replica was unsynced. As a result, a freshly-recovered
# replica that was actually fully caught up was marked as unsynced, and the
# `chassert(our_log_ptr < max_log_ptr)` in `initAndCheckTask` would fire as
# soon as `our_log_ptr` reached `max_log_ptr` while `unsynced_after_recovery`
# was still set.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="rdb_zero_lag_${CLICKHOUSE_DATABASE}"

# Silence Replicated DDL status rows to keep the test output deterministic.
CH="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none"

${CH} -q "DROP DATABASE IF EXISTS ${db} SYNC"

# Create the Replicated database with the zero replication-lag threshold that
# triggered the assertion in the issue.
${CH} -q "
    CREATE DATABASE ${db}
    ENGINE = Replicated('/test/${CLICKHOUSE_DATABASE}/rdb_zero_lag', 's1', 'r1')
    SETTINGS max_replication_lag_to_enqueue = 0
"

# Reproduce the exact pattern from the bug report: an intermediate DDL that
# fails at parse time (so it never reaches the DDL queue) followed by a normal
# DDL. With `--ignore-error` the client keeps going through the parse failure,
# matching the original reproducer.
${CH} --ignore-error -q "
    CREATE TABLE ${db}.t0 ENGINE = Join(SEMI, RIGHT, c0);
    CREATE TABLE ${db}.t1 (c0 Int) ENGINE = MergeTree ORDER BY tuple();
" 2>/dev/null

# Also exercise the normal happy path that previously tripped the
# `our_log_ptr < max_log_ptr` assertion as soon as the replica caught up.
${CH} -q "CREATE TABLE ${db}.t2 (s String) ENGINE = MergeTree ORDER BY s"
${CH} -q "INSERT INTO ${db}.t2 VALUES ('a'), ('b')"

# If the server is still up and the data is correct, the bug is fixed.
${CH} -q "SELECT count() FROM ${db}.t1"
${CH} -q "SELECT count() FROM ${db}.t2"

${CH} -q "DROP DATABASE IF EXISTS ${db} SYNC"

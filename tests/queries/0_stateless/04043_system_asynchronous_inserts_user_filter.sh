#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: the test relies on a pending async insert remaining in the queue;
# concurrent `SYSTEM FLUSH ASYNC INSERT QUEUE` from other tests can drain it.

# Regression test: system.asynchronous_inserts must not leak cross-user insert metadata.
# A user without SHOW_USERS privilege must only see their own pending inserts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS secret_user_${CLICKHOUSE_DATABASE};
    DROP USER IF EXISTS restricted_user_${CLICKHOUSE_DATABASE};
    CREATE USER secret_user_${CLICKHOUSE_DATABASE};
    CREATE USER restricted_user_${CLICKHOUSE_DATABASE};
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.async_insert_test;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.async_insert_test (x UInt64) ENGINE=MergeTree ORDER BY x;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.async_insert_test TO secret_user_${CLICKHOUSE_DATABASE};
    GRANT SELECT ON system.asynchronous_inserts TO secret_user_${CLICKHOUSE_DATABASE};
    GRANT SELECT ON system.asynchronous_inserts TO restricted_user_${CLICKHOUSE_DATABASE};
"

# secret_user inserts with async_insert enabled and a very long flush timeout so the entry stays in the queue.
# Disable adaptive busy timeout so the per-query timeout is honored exactly and the entry is not
# scheduled for early flush by `max_busy_timeout_exceeded` when the shard has been idle for a long time.
${CLICKHOUSE_CLIENT} \
    --user "secret_user_${CLICKHOUSE_DATABASE}" \
    --async_insert 1 \
    --async_insert_use_adaptive_busy_timeout 0 \
    --async_insert_busy_timeout_max_ms 600000 \
    --async_insert_busy_timeout_min_ms 600000 \
    --wait_for_async_insert 0 \
    -q "INSERT INTO ${CLICKHOUSE_DATABASE}.async_insert_test VALUES (42)"

# Wait until the admin can see the pending entry. With `--wait_for_async_insert 0` the client returns
# as soon as the entry is queued, but in slow CI environments (sanitizers) the visibility through
# `system.asynchronous_inserts` may lag slightly; poll for up to 30 seconds before giving up.
for _ in $(seq 1 300); do
    admin_count=$(${CLICKHOUSE_CLIENT} \
        -q "SELECT count() FROM system.asynchronous_inserts WHERE table = 'async_insert_test' AND database = '${CLICKHOUSE_DATABASE}'")
    if [[ "${admin_count}" -ge 1 ]]; then
        break
    fi
    sleep 0.1
done

# restricted_user must see 0 rows (no cross-user visibility).
echo "restricted_user sees:"
${CLICKHOUSE_CLIENT} \
    --user "restricted_user_${CLICKHOUSE_DATABASE}" \
    -q "SELECT count() FROM system.asynchronous_inserts WHERE table = 'async_insert_test' AND database = '${CLICKHOUSE_DATABASE}'"

# secret_user must see their own row.
echo "secret_user sees:"
${CLICKHOUSE_CLIENT} \
    --user "secret_user_${CLICKHOUSE_DATABASE}" \
    -q "SELECT count() FROM system.asynchronous_inserts WHERE table = 'async_insert_test' AND database = '${CLICKHOUSE_DATABASE}'"

# Admin (current session) must see all rows.
echo "admin sees:"
${CLICKHOUSE_CLIENT} \
    -q "SELECT count() FROM system.asynchronous_inserts WHERE table = 'async_insert_test' AND database = '${CLICKHOUSE_DATABASE}'"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS secret_user_${CLICKHOUSE_DATABASE};
    DROP USER IF EXISTS restricted_user_${CLICKHOUSE_DATABASE};
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.async_insert_test;
"

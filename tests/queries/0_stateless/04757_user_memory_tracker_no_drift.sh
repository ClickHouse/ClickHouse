#!/usr/bin/env bash
# Memory a query allocates but does not free itself must not stay charged to its user. The check needs a query of
# the same user running the whole time, because otherwise the per-user tracker starts a new period once the user
# has no queries left, and any drift would be hidden.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_${CLICKHOUSE_DATABASE}"
# Batched, like the groups of statements below: starting a client is expensive enough in a sanitizer build to
# dominate this test, and none of these need to be a separate invocation.
${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password;
    GRANT SELECT, INSERT, CREATE, DROP, TRUNCATE, OPTIMIZE, ALTER ON $CLICKHOUSE_DATABASE.* TO $user;
    CREATE TABLE mem (k UInt64, s String) ENGINE = Memory;
    CREATE TABLE mt (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE jn (k UInt64, s String) ENGINE = Join(ANY, LEFT, k)"

# Keeps the user active, and holds next to nothing itself, for as long as the queries below run. Five minutes,
# which is far longer than they take even on a loaded machine with a debug build, and it is killed on the way out.
${CLICKHOUSE_CLIENT} --user "$user" --function_sleep_max_microseconds_per_block 0 -q "
    SELECT count() FROM numbers(1500) WHERE sleepEachRow(0.2) = 0 SETTINGS max_block_size = 1 FORMAT Null" &
holder=$!
trap 'kill $holder 2>/dev/null' EXIT

# How many queries the user has running, and what its tracker holds, read together.
snapshot() {
    ${CLICKHOUSE_CLIENT} -q "SELECT
        (SELECT count() FROM system.processes WHERE user = '$user'),
        (SELECT ifNull(max(memory_usage), 0) FROM system.user_processes WHERE user = '$user')"
}

base=0
for _ in {1..200}; do
    read -r running held < <(snapshot)
    # Wait for the memory too, not just for the query to show up: the user's row appears as soon as it has a
    # query, a moment before anything is charged to it, and a base of zero would fail the check below.
    if [ "${running:-0}" -ge 1 ] && [ "${held:-0}" -gt 0 ]; then
        base=$held
        break
    fi
    sleep 0.2
done

# Without a query of this user running, the tracker would start a new period and hide any drift.
if [ "$base" -le 0 ]; then
    echo "the holding query never started"
    exit 1
fi

# `< /dev/null`, or the client waits for data on an inherited stdin that never reaches EOF, while the server waits
# for that same data, and the insert never completes.
${CLICKHOUSE_CLIENT} --user "$user" --async_insert 0 -q "
    INSERT INTO mem VALUES (1, 'x');
    INSERT INTO mem VALUES (1, 'x');
    INSERT INTO mem VALUES (1, 'x');
    TRUNCATE TABLE mem" < /dev/null

# The data left in the tables is on purpose: it outlives the query that wrote it, and belongs to the server from
# then on. The join table is then rewritten whole, dropping data the server owns and building its replacement.
${CLICKHOUSE_CLIENT} --user "$user" --async_insert 0 -q "
    INSERT INTO mem SELECT number, repeat(toString(number), 40) FROM numbers(40000);
    INSERT INTO mt SELECT number, toString(number) FROM numbers(40000);
    INSERT INTO jn SELECT number, repeat(toString(number), 20) FROM numbers(40000);
    ALTER TABLE jn DELETE WHERE k = 1;
    SELECT sum(k) FROM mt SETTINGS use_uncompressed_cache = 1 FORMAT Null;
    OPTIMIZE TABLE mt FINAL"

# Only what the holder itself works on should still be charged to this user, a few hundred kilobytes.
read -r running after < <(snapshot)
if [ "$running" -lt 1 ]; then
    echo "the holding query ended before the drift could be measured"
    exit 1
fi

drift=$(( after - base ))
if [ "${drift#-}" -lt 1500000 ]; then
    echo "the user is charged for what it holds"
else
    echo "the user drifted by $drift bytes"
fi

# Data waiting in the queue is still the user's: it must count against `max_memory_usage_for_user` until flushed.
data_file="${CLICKHOUSE_TMP}/04757_${CLICKHOUSE_DATABASE}.csv"
${CLICKHOUSE_CLIENT} -q "SELECT number, repeat('x', 100) FROM numbers(40000) FORMAT CSV" > "$data_file"
read -r running queued_base < <(snapshot)
${CLICKHOUSE_CLIENT} --user "$user" --async_insert 1 --wait_for_async_insert 0 \
    --async_insert_busy_timeout_min_ms 600000 --async_insert_busy_timeout_max_ms 600000 \
    --async_insert_use_adaptive_busy_timeout 0 \
    --async_insert_max_data_size 1000000000 --async_insert_max_query_number 1000000 \
    -q "INSERT INTO mt FORMAT CSV" < "$data_file"

read -r running held < <(snapshot)
if [ "$running" -lt 1 ]; then
    echo "the holding query ended before the queued data could be measured"
    exit 1
fi
pending=$(( held - queued_base ))
if [ "$pending" -gt 1500000 ]; then
    echo "the data waiting in the queue counts against the user"
else
    echo "the data waiting in the queue counts $pending bytes"
fi

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE mt"
read -r running held < <(snapshot)
if [ "$running" -lt 1 ]; then
    echo "the holding query ended before the flush could be measured"
    exit 1
fi
flushed=$(( held - queued_base ))
if [ "${flushed#-}" -lt 1500000 ]; then
    echo "the flushed data stops counting against the user"
else
    echo "the flushed data still counts $flushed bytes"
fi

rm -f "$data_file"
kill $holder 2>/dev/null
wait $holder 2>/dev/null
${CLICKHOUSE_CLIENT} -q "DROP USER $user"

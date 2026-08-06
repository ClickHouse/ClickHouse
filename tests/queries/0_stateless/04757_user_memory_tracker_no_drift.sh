#!/usr/bin/env bash
# Memory a query allocates but does not free itself must not stay charged to its user. The check needs a query of
# the same user running the whole time, because otherwise the per-user tracker starts a new period once the user
# has no queries left, and any drift would be hidden.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} -q "CREATE USER $user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, INSERT, CREATE, DROP, TRUNCATE, OPTIMIZE ON $CLICKHOUSE_DATABASE.* TO $user"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE mem (k UInt64, s String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE mt (k UInt64, s String) ENGINE = MergeTree ORDER BY k"

# Keeps the user active, and holds next to nothing itself, for as long as the queries below run.
${CLICKHOUSE_CLIENT} --user "$user" --function_sleep_max_microseconds_per_block 0 -q "
    SELECT count() FROM numbers(400) WHERE sleepEachRow(0.2) = 0 SETTINGS max_block_size = 1 FORMAT Null" &
holder=$!
trap 'kill $holder 2>/dev/null' EXIT

running() {
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE user = '$user'"
}

floor() {
    ${CLICKHOUSE_CLIENT} -q "SELECT ifNull(max(memory_usage), 0) FROM system.user_processes WHERE user = '$user'"
}

base=0
for _ in {1..200}; do
    if [ "$(running)" -ge 1 ]; then
        base=$(floor)
        break
    fi
    sleep 0.2
done

# Without a query of this user running, the tracker would start a new period and hide any drift.
if [ "$base" -le 0 ]; then
    echo "the holding query never started"
    exit 1
fi

for _ in {1..10}; do
    ${CLICKHOUSE_CLIENT} --user "$user" --async_insert 0 -q "INSERT INTO mem VALUES (1, 'x')"
done
${CLICKHOUSE_CLIENT} --user "$user" -q "TRUNCATE TABLE mem"
# Left in the table on purpose: the data outlives the query that wrote it, and belongs to the server from then on.
${CLICKHOUSE_CLIENT} --user "$user" --async_insert 0 -q "INSERT INTO mem SELECT number, repeat(toString(number), 4) FROM numbers(500000)"
${CLICKHOUSE_CLIENT} --user "$user" --async_insert 0 -q "INSERT INTO mt SELECT number, toString(number) FROM numbers(200000)"
${CLICKHOUSE_CLIENT} --user "$user" --use_uncompressed_cache 1 -q "SELECT sum(k) FROM mt FORMAT Null"
${CLICKHOUSE_CLIENT} --user "$user" -q "OPTIMIZE TABLE mt FINAL"

# Only what the holder itself works on should still be charged to this user, a few hundred kilobytes.
if [ "$(running)" -lt 1 ]; then
    echo "the holding query ended before the drift could be measured"
    exit 1
fi
after=$(floor)

drift=$(( after - base ))
if [ "${drift#-}" -lt 4000000 ]; then
    echo "the user is charged for what it holds"
else
    echo "the user drifted by $drift bytes"
fi

# Data waiting in the queue is still the user's: it must count against `max_memory_usage_for_user` until flushed.
data_file="${CLICKHOUSE_TMP}/04757_${CLICKHOUSE_DATABASE}.csv"
${CLICKHOUSE_CLIENT} -q "SELECT number, repeat('x', 100) FROM numbers(20000) FORMAT CSV" > "$data_file"
for _ in {1..5}; do
    ${CLICKHOUSE_CLIENT} --user "$user" --async_insert 1 --wait_for_async_insert 0 \
        --async_insert_busy_timeout_min_ms 60000 --async_insert_busy_timeout_max_ms 60000 \
        --async_insert_use_adaptive_busy_timeout 0 \
        --async_insert_max_data_size 1000000000 --async_insert_max_query_number 1000000 \
        -q "INSERT INTO mt FORMAT CSV" < "$data_file"
done

pending=$(( $(floor) - after ))
if [ "$pending" -gt 5000000 ]; then
    echo "the data waiting in the queue counts against the user"
else
    echo "the data waiting in the queue counts $pending bytes"
fi

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE mt"
flushed=$(( $(floor) - after ))
if [ "${flushed#-}" -lt 4000000 ]; then
    echo "the flushed data stops counting against the user"
else
    echo "the flushed data still counts $flushed bytes"
fi

rm -f "$data_file"
kill $holder 2>/dev/null
wait $holder 2>/dev/null
${CLICKHOUSE_CLIENT} -q "DROP USER $user"

#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `kill_throw_if_noop` is enabled by default. `05233_kill_throw_if_noop.sql` covers the absent
# query_id / mutation_id cases; this test covers the rest of what the former integration suite
# `test_kill_throw_if_noop` exercised: `user = currentUser()` matching nothing (sections 1-2),
# killing a running query without throwing (section 4), `KILL MUTATION` with an empty match
# (sections 5-6), and the `TEST` dry runs staying previews instead of throwing (sections 3, 7).
# A dedicated, otherwise idle user keeps the `currentUser()` outcome deterministic while
# stateless tests run in parallel.

ID="${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER="kq_$ID"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $USER"
$CLICKHOUSE_CLIENT -q "CREATE USER $USER IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.numbers TO $USER"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(query_id, user, query) ON system.processes TO $USER"

echo "-- 1. user = currentUser() matches only the caller's own rows, and the KILL statement itself is excluded; with the dedicated user idle otherwise, the match is empty and the default throws"
OUT=$($CLICKHOUSE_CLIENT --user "$USER" -q "KILL QUERY WHERE user = currentUser() SETTINGS kill_throw_if_noop=1" 2>&1)
if grep -q -F "NOTHING_TO_KILL" <<< "$OUT"; then
    echo "throws NOTHING_TO_KILL"
else
    echo "unexpected: $OUT"
fi

echo "-- 2. the same empty match with the setting off"
OUT=$($CLICKHOUSE_CLIENT --user "$USER" -q "KILL QUERY WHERE user = currentUser() SETTINGS kill_throw_if_noop = 0" 2>&1)
if [[ "$(echo -n "$OUT" | grep -c .)" == "0" ]]; then
    echo "no rows, no error"
else
    echo "unexpected: $OUT"
fi

echo "-- 3. TEST is a dry run: an empty match is an empty preview, not an exception"
OUT=$($CLICKHOUSE_CLIENT --user "$USER" -q "KILL QUERY WHERE user = currentUser() TEST" 2>&1)
if [[ "$(echo -n "$OUT" | grep -c .)" == "0" ]]; then
    echo "empty preview"
else
    echo "unexpected: $OUT"
fi

echo "-- 4. killing a live query must not throw with the default setting and must stop it"
$CLICKHOUSE_CLIENT --query_id "live_$ID" -q \
    "SELECT sleep(0.1) FROM numbers(100000) SETTINGS max_block_size = 1, max_rows_to_read = 0" \
    > /dev/null 2>&1 &
for _ in {1..150}; do
    if [[ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = 'live_$ID'")" == "1" ]]; then
        break
    fi
    sleep 0.2
done

OUT=$($CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = 'live_$ID' SYNC" 2>&1)
echo "rows: $(echo -n "$OUT" | grep -c .)"

GONE=no
for _ in {1..150}; do
    if [[ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = 'live_$ID'")" == "0" ]]; then
        GONE=yes
        break
    fi
    sleep 0.2
done
if [[ "$GONE" == "yes" ]]; then
    echo "victim: gone"
else
    echo "victim: still running"
fi

echo "-- 5. KILL MUTATION with an empty match throws by default"
OUT=$($CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE mutation_id = 'kq_$ID' SETTINGS kill_throw_if_noop=1" 2>&1)
if grep -q -F "NOTHING_TO_KILL" <<< "$OUT"; then
    echo "throws NOTHING_TO_KILL"
else
    echo "unexpected: $OUT"
fi

echo "-- 6. the same empty match with the setting off"
OUT=$($CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE mutation_id = 'kq_$ID' SETTINGS kill_throw_if_noop = 0" 2>&1)
if [[ "$(echo -n "$OUT" | grep -c .)" == "0" ]]; then
    echo "no rows, no error"
else
    echo "unexpected: $OUT"
fi

echo "-- 7. KILL MUTATION TEST with nothing to kill is an empty preview"
OUT=$($CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE mutation_id = 'kq_$ID' TEST" 2>&1)
if [[ "$(echo -n "$OUT" | grep -c .)" == "0" ]]; then
    echo "empty preview"
else
    echo "unexpected: $OUT"
fi

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $USER"
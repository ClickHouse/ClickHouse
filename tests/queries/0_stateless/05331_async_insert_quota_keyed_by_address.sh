#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An async insert flush must bill `written_bytes` to the bucket of the client that sent the data,
# for quotas keyed by the client address or by the X-Forwarded-For address.

D=${CLICKHOUSE_DATABASE}
U1="u1_$D"; Q1="q1_$D"
U2="u2_$D"; Q2="q2_$D"
U3A="u3a_$D"; Q3A="q3a_$D"
U3B="u3b_$D"; Q3B="q3b_$D"
U4="u4_$D"; Q4="q4_$D"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $U1, $U2, $U3A, $U3B, $U4"
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS $Q1, $Q2, $Q3A, $Q3B, $Q4"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t1 (x UInt64, s String) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t2 (x UInt64, s String) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t3 (x UInt64, s String) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t4 (x UInt64, s String) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE USER $U1, $U2, $U3A, $U3B, $U4 IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT, SELECT ON $D.t1 TO $U1"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON $D.t2 TO $U2"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON $D.t3 TO $U3A, $U3B"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON $D.t4 TO $U4"

ASYNC="async_insert=1&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_min_ms=50&async_insert_max_data_size=100000000&async_insert_max_query_number=1000&async_insert_deduplicate=0"
WAIT="$ASYNC&wait_for_async_insert=1&async_insert_busy_timeout_max_ms=100"
# Flushed only by SYSTEM FLUSH ASYNC INSERT QUEUE.
NOWAIT="$ASYNC&wait_for_async_insert=0&async_insert_busy_timeout_max_ms=600000"

function http_insert()
{
    local user=$1 xff=$2 params=$3 query=$4
    local out headers=()
    [ -n "$xff" ] && headers=(-H "X-Forwarded-For: $xff")
    out=$(${CLICKHOUSE_CURL} -sS "${headers[@]}" "${CLICKHOUSE_URL}&user=$user&$params" --data-binary "$query" 2>&1)
    if [ -z "$out" ]; then echo 'OK'; elif [[ "$out" == *QUOTA_EXCEEDED* ]]; then echo 'QUOTA_EXCEEDED'; else echo "$out"; fi
}

echo "--- KEYED BY ip_address"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA $Q1 KEYED BY ip_address FOR INTERVAL 100 YEAR MAX written_bytes = 1000000000 TO $U1"
# Data sent as blocks over the native protocol, inlined data over HTTP, and INSERT ... SELECT.
${CLICKHOUSE_CLIENT} --user "$U1" --async_insert 1 --wait_for_async_insert 1 --async_insert_use_adaptive_busy_timeout 0 \
    --async_insert_busy_timeout_max_ms 100 --async_insert_deduplicate 0 -q "INSERT INTO t1 FORMAT TSV" <<< $'1\tnative'
http_insert "$U1" "" "$WAIT" "INSERT INTO t1 VALUES (2, 'http')"
http_insert "$U1" "" "$WAIT&async_insert_select_as_async_insert=1" "INSERT INTO t1 SELECT toUInt64(3), 'select'"
${CLICKHOUSE_CLIENT} -q "SELECT x, s FROM t1 ORDER BY x"
# Every bucket that counted the queries also counted their written bytes, and nothing went to 0.0.0.0.
${CLICKHOUSE_CLIENT} -q "
    SELECT countIf(quota_key = '0.0.0.0' OR (queries > 0) != (written_bytes > 0)), countIf(written_bytes > 0) > 0
    FROM system.quotas_usage WHERE quota_name = '$Q1'"

echo "--- KEYED BY client_key, ip_address"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA $Q4 KEYED BY client_key, ip_address FOR INTERVAL 100 YEAR MAX written_bytes = 1000000000 TO $U4"
# Without a client key the quota is keyed by the client address, with one by the key.
http_insert "$U4" "" "$WAIT" "INSERT INTO t4 VALUES (1, 'no key')"
http_insert "$U4" "" "$WAIT&quota_key=tenant_$D" "INSERT INTO t4 VALUES (2, 'key')"
${CLICKHOUSE_CLIENT} -q "
    SELECT countIf(quota_key = '0.0.0.0' OR (queries > 0) != (written_bytes > 0)), countIf(quota_key = 'tenant_$D' AND written_bytes > 0)
    FROM system.quotas_usage WHERE quota_name = '$Q4'"

echo "--- KEYED BY forwarded_ip_address"
# Room for one row of 316 bytes per client.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA $Q2 KEYED BY forwarded_ip_address FOR INTERVAL 100 YEAR MAX written_bytes = 500 TO $U2"
PAD=$(printf 'x%.0s' $(seq 1 300))
http_insert "$U2" 10.0.0.1 "$WAIT" "INSERT INTO t2 VALUES (1, '$PAD')"
http_insert "$U2" 10.0.0.1 "$WAIT" "INSERT INTO t2 VALUES (2, '$PAD')"
# Another client is not affected by the first one's consumption.
http_insert "$U2" 10.0.0.2 "$WAIT" "INSERT INTO t2 VALUES (3, '$PAD')"
http_insert "$U2" 10.0.0.3 "$NOWAIT" "INSERT INTO t2 VALUES (4, '$PAD')"
# The exhausted client is rejected before its data is acknowledged.
http_insert "$U2" 10.0.0.1 "$NOWAIT" "INSERT INTO t2 VALUES (5, '$PAD')"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE t2"
${CLICKHOUSE_CLIENT} -q "SELECT groupArray(x) FROM (SELECT x FROM t2 ORDER BY x)"
${CLICKHOUSE_CLIENT} -q "SELECT quota_key FROM system.quotas_usage WHERE quota_name = '$Q2' AND written_bytes > 0 ORDER BY quota_key"

echo "--- batching"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA $Q3A KEYED BY user_name FOR INTERVAL 100 YEAR MAX written_bytes = 1000000000 TO $U3A"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA $Q3B KEYED BY forwarded_ip_address FOR INTERVAL 100 YEAR MAX written_bytes = 1000000000 TO $U3B"
http_insert "$U3A" 10.0.0.1 "$NOWAIT&query_id=${D}_a1" "INSERT INTO t3 VALUES (1, 'a1')"
http_insert "$U3A" 10.0.0.2 "$NOWAIT&query_id=${D}_a2" "INSERT INTO t3 VALUES (2, 'a2')"
http_insert "$U3B" 10.0.0.1 "$NOWAIT&query_id=${D}_b1" "INSERT INTO t3 VALUES (3, 'b1')"
http_insert "$U3B" 10.0.0.2 "$NOWAIT&query_id=${D}_b2" "INSERT INTO t3 VALUES (4, 'b2')"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE t3"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t3"
LOG_FILTER="database = currentDatabase() AND query_id IN ('${D}_a1', '${D}_a2', '${D}_b1', '${D}_b2')"
for _ in {1..20}; do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
    [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.asynchronous_insert_log WHERE $LOG_FILTER")" = 4 ] && break
    sleep 0.5
done
# Clients of a quota that is not keyed by address still share a flush; the others get one each.
${CLICKHOUSE_CLIENT} -q "
    SELECT substring(query_id, length('$D') + 2, 1) AS user, uniqExact(flush_query_id)
    FROM system.asynchronous_insert_log WHERE $LOG_FILTER
    GROUP BY user ORDER BY user"
${CLICKHOUSE_CLIENT} -q "SELECT quota_key FROM system.quotas_usage WHERE quota_name = '$Q3B' AND written_bytes > 0 ORDER BY quota_key"

cleanup

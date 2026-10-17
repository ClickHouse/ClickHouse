#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Custom-key parallel replicas ship the whole query to every replica, and `input()` is a one-shot
# stream that only exists on the initiator, so an `INSERT SELECT` filtered by `input()` used to fail
# with INVALID_USAGE_OF_INPUT ("Input stream is not initialized") from a follower.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_ck_input"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_ck_input (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 16"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_ck_input SELECT number, number * 2 FROM numbers(1000)"

urlencode() {
    python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

# `serialize_query_plan = 0`: custom key with plan serialization throws NOT_IMPLEMENTED.
# `automatic_parallel_replicas_mode = 0`: the test runner injects 2 with probability 0.25 and then
# also rewrites `cluster_for_parallel_replicas`.
settings() {
    echo "SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3\
, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost'\
, parallel_replicas_mode = '$1'\
, parallel_replicas_custom_key = 'k'\
, parallel_replicas_for_non_replicated_merge_tree = 1\
, prefer_localhost_replica = 0\
, serialize_query_plan = 0\
, automatic_parallel_replicas_mode = 0"
}

# The arms after the loop flip one setting that `settings` pins. A rename inside `settings` would turn the
# `sed` into a no-op and the arm would then assert the pinned default, so fail loudly instead. It runs in
# a command substitution, so it cannot exit the test; it emits invalid SQL instead.
settings_with() {
    local out
    out=$(settings 'custom_key_sampling' | sed "s/$1/$2/")
    case "$out" in
        *"$2"*) printf '%s' "$out" ;;
        *) echo "FATAL: settings() no longer contains '$1'" >&2; printf 'SETTINGS_FLIP_FAILED' ;;
    esac
}

for mode in 'custom_key_sampling' 'custom_key_range'; do
    echo "mode=$mode"

    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dst_http"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE dst_http (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"
    Q=$(urlencode "INSERT INTO dst_http $(settings "$mode") SELECT k, v FROM t_ck_input WHERE k IN (SELECT n FROM input('n UInt64')) FORMAT JSONEachRow")
    printf '%s\n' '{"n": 7}' '{"n": 19}' '{"n": 123}' \
        | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=${Q}" --data-binary @-
    echo -n 'http rows: '
    $CLICKHOUSE_CLIENT -q "SELECT groupArray((k, v)) FROM (SELECT k, v FROM dst_http ORDER BY ALL)"

    # The TCP path installs an input callback instead of setting the pipe directly,
    # so it reaches the other arm of StorageInput::readImpl.
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dst_tcp"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE dst_tcp (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"
    printf '%s\n' '{"n": 7}' '{"n": 19}' '{"n": 123}' \
        | $CLICKHOUSE_CLIENT -q "INSERT INTO dst_tcp $(settings "$mode") SELECT k, v FROM t_ck_input WHERE k IN (SELECT n FROM input('n UInt64')) FORMAT JSONEachRow"
    echo -n 'tcp rows: '
    $CLICKHOUSE_CLIENT -q "SELECT groupArray((k, v)) FROM (SELECT k, v FROM dst_tcp ORDER BY ALL)"

    # Engagement guard: the assertions above pass vacuously if custom-key mode does not engage at
    # all on this server. A query that does not read `input()` must still be split across replicas.
    # Fresh per-invocation query_id (initial_query_id propagates to the remote sub-queries) plus a
    # tight event_time bound isolate the guard from earlier runs in the same database.
    query_id="05238_ck_engage_${mode}_${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}"
    start_time=$($CLICKHOUSE_CLIENT -q "SELECT now()")
    $CLICKHOUSE_CLIENT --query_id="$query_id" -q "SELECT sum(v) FROM t_ck_input WHERE k % 3 = 1 $(settings "$mode") FORMAT Null"
    # A replica writes its own query_log row after the initiator's query has already returned,
    # so poll instead of reading once. A read that never reaches 2 still prints 0 and fails.
    engaged=0
    for _ in $(seq 1 30); do
        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
        engaged=$($CLICKHOUSE_CLIENT -q "
            SELECT countDistinct(query) > 1
            FROM system.query_log
            WHERE has(databases, currentDatabase()) AND initial_query_id = '$query_id'
              AND is_initial_query = 0 AND type = 'QueryFinish'
              AND event_date >= yesterday() AND event_time >= '$start_time'")
        [ "$engaged" = 1 ] && break
        sleep 0.5
    done
    echo "engaged remote replicas > 1: $engaged"
done

# `enable_parallel_replicas = 2` asks to fail rather than fall back, but the decline runs before storage
# eligibility is known, so it clears the setting instead of throwing: a query whose only source is
# `input()` has nothing to distribute and has to keep working.
force_settings=$(settings_with 'enable_parallel_replicas = 1' 'enable_parallel_replicas = 2')

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dst_force"
$CLICKHOUSE_CLIENT -q "CREATE TABLE dst_force (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"
printf '%s\n' '{"n": 7}' '{"n": 19}' '{"n": 123}' \
    | $CLICKHOUSE_CLIENT -q "INSERT INTO dst_force $force_settings SELECT k, v FROM t_ck_input WHERE k IN (SELECT n FROM input('n UInt64')) FORMAT JSONEachRow"
echo -n 'force mode rows: '
$CLICKHOUSE_CLIENT -q "SELECT groupArray((k, v)) FROM (SELECT k, v FROM dst_force ORDER BY ALL)"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dst_direct"
$CLICKHOUSE_CLIENT -q "CREATE TABLE dst_direct (n UInt64) ENGINE = MergeTree ORDER BY n"
printf '%s\n' '{"n": 7}' '{"n": 19}' '{"n": 123}' \
    | $CLICKHOUSE_CLIENT -q "INSERT INTO dst_direct $force_settings SELECT n FROM input('n UInt64') FORMAT JSONEachRow"
echo -n 'force mode direct input rows: '
$CLICKHOUSE_CLIENT -q "SELECT groupArray(n) FROM (SELECT n FROM dst_direct ORDER BY ALL)"

# An `input()` the analyzer never resolves is still an `input()`: `view()` marks its query argument as
# skipped, so the node inside it has no storage to match on.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dst_wrapped"
$CLICKHOUSE_CLIENT -q "CREATE TABLE dst_wrapped (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"
printf '%s\n' '{"n": 7}' '{"n": 19}' '{"n": 123}' \
    | $CLICKHOUSE_CLIENT -q "INSERT INTO dst_wrapped $(settings 'custom_key_sampling') SELECT k, v FROM t_ck_input WHERE k IN (SELECT n FROM view(SELECT n FROM input('n UInt64'))) FORMAT JSONEachRow"
echo -n 'wrapped input rows: '
$CLICKHOUSE_CLIENT -q "SELECT groupArray((k, v)) FROM (SELECT k, v FROM dst_wrapped ORDER BY ALL)"

# A reused MATERIALIZED CTE is evaluated on the initiator and sent as an external table, so no replica
# reads its input(): such a query keeps its custom-key split. A single-use one is inlined instead and
# is still declined, which the rows oracle alone cannot see, so that case is measured out of tree.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dst_mcte"
$CLICKHOUSE_CLIENT -q "CREATE TABLE dst_mcte (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"
mcte_query_id="05238_ck_mcte_${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}"
mcte_start_time=$($CLICKHOUSE_CLIENT -q "SELECT now()")
printf '%s\n' '{"n": 7}' '{"n": 19}' '{"n": 123}' \
    | $CLICKHOUSE_CLIENT --query_id="$mcte_query_id" -q "INSERT INTO dst_mcte WITH c AS MATERIALIZED (SELECT n FROM input('n UInt64')) SELECT k, v FROM t_ck_input WHERE k IN (SELECT n FROM c) AND k NOT IN (SELECT n + 1000000 FROM c) $(settings 'custom_key_sampling'), enable_materialized_cte = 1 FORMAT JSONEachRow"
echo -n 'materialized cte rows: '
$CLICKHOUSE_CLIENT -q "SELECT groupArray((k, v)) FROM (SELECT k, v FROM dst_mcte ORDER BY ALL)"
mcte_engaged=0
for _ in $(seq 1 30); do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    mcte_engaged=$($CLICKHOUSE_CLIENT -q "
        SELECT countDistinct(query) > 1
        FROM system.query_log
        WHERE has(databases, currentDatabase()) AND initial_query_id = '$mcte_query_id'
          AND is_initial_query = 0 AND type = 'QueryFinish'
          AND event_date >= yesterday() AND event_time >= '$mcte_start_time'")
    [ "$mcte_engaged" = 1 ] && break
    sleep 0.5
done
echo "materialized cte engaged remote replicas > 1: $mcte_engaged"

# The decline runs before ClusterProxy::executeQueryWithParallelReplicasCustomKey, which rejects custom key
# with a serialized plan, so this combination now runs locally instead of throwing NOT_IMPLEMENTED.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dst_serialized"
$CLICKHOUSE_CLIENT -q "CREATE TABLE dst_serialized (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"
serialized_settings=$(settings_with 'serialize_query_plan = 0' 'serialize_query_plan = 1')
printf '%s\n' '{"n": 7}' '{"n": 19}' '{"n": 123}' \
    | $CLICKHOUSE_CLIENT -q "INSERT INTO dst_serialized $serialized_settings SELECT k, v FROM t_ck_input WHERE k IN (SELECT n FROM input('n UInt64')) FORMAT JSONEachRow"
echo -n 'serialize_query_plan rows: '
$CLICKHOUSE_CLIENT -q "SELECT groupArray((k, v)) FROM (SELECT k, v FROM dst_serialized ORDER BY ALL)"

$CLICKHOUSE_CLIENT -q "DROP TABLE dst_http"
$CLICKHOUSE_CLIENT -q "DROP TABLE dst_tcp"
$CLICKHOUSE_CLIENT -q "DROP TABLE dst_force"
$CLICKHOUSE_CLIENT -q "DROP TABLE dst_direct"
$CLICKHOUSE_CLIENT -q "DROP TABLE dst_wrapped"
$CLICKHOUSE_CLIENT -q "DROP TABLE dst_mcte"
$CLICKHOUSE_CLIENT -q "DROP TABLE dst_serialized"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_ck_input"

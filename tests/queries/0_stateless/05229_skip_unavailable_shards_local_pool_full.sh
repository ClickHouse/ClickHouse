#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A shard is missing from the result here because the initiator had no free connection slot for its
# only replica, so that replica was never contacted. That is not an unavailable shard, and
# skip_unavailable_shards must not silence it: a two shard SELECT would otherwise return one shard's
# rows with exit code 0.
#
# The arms run at both use_hedged_requests values, because the two connection allocators are separate
# code paths (HedgedConnectionsFactory and PoolWithFailoverBase) and each interprets a zero minimum
# entry count on its own.

# The pool is keyed on host, port, credentials and pool size, so a run gets a pool of its own by
# connecting as a user of its own. That is what lets concurrent copies of this test hold their own
# connection instead of queueing behind one, which is what no-parallel would otherwise be for.
USER="u_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${USER} IDENTIFIED WITH no_password"
# INSERT is for arm C, whose write leg runs on the remote side as this user.
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${USER}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t SELECT number FROM numbers(10)"

# Written by arm C only.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x"

# Read by the holder only. Thirty one second rows outlive every arm's own bound, so the connection
# stays out of the pool until the holder is killed rather than until it runs out of rows.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE hold (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query "INSERT INTO hold SELECT number FROM numbers(30)"

ADDR="127.0.0.1:${CLICKHOUSE_PORT_TCP}"
ALT="localhost:${CLICKHOUSE_PORT_TCP}"
PREFIX="05229_${CLICKHOUSE_DATABASE}_$(random_str 8)"
# distributed_connections_pool_size is part of the pool key, so the holder and the victim only
# contend when they agree on it.
COMMON="prefer_localhost_replica = 0, distributed_connections_pool_size = 1, enable_parallel_replicas = 0"

HOLDER_PID=""

# A query is in system.processes from the moment it is registered, which is before its
# RemoteQueryExecutor takes a connection out of the pool, so the holder being listed does not mean the
# pool is full yet. Its secondary query exists only once a connection was taken and the query was sent
# on it, which makes that the occupancy itself.
function holding() {
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM system.processes
        WHERE initial_query_id = '${1}' AND NOT is_initial_query"
}

# Bounded by wall clock rather than by a number of attempts: one poll spawns a client, which is the
# dominant cost here and takes seconds under a sanitizer, so an attempt count bounds no amount of
# time. A condition that can no longer hold has to be reported, well inside the test budget.
function wait_holding() {
    local id=$1 deadline=$((SECONDS + $2))
    while (( SECONDS < deadline )); do
        [[ $(holding "${id}") != 0 ]] && return 0
        sleep 0.05
    done
    return 1
}

# Takes the single connection of the ADDR pool and keeps it. connection_pool_max_wait_ms = 0 is the
# default, so the holder itself never waits on a finite deadline and cannot be the thing that fails.
function hold_pool() {
    local holder="${PREFIX}_${1}_holder"
    timeout 120 ${CLICKHOUSE_CLIENT} --query_id "${holder}" --query "
        SELECT count() FROM remote('${ADDR}', '${CLICKHOUSE_DATABASE}', 'hold', '${USER}', '')
        WHERE sleepEachRow(1)
        SETTINGS ${COMMON}, connection_pool_max_wait_ms = 0, use_hedged_requests = 0,
                 function_sleep_max_microseconds_per_block = 60000000
    " < /dev/null > /dev/null 2>&1 &
    HOLDER_PID=$!

    # The victim starts once the holder owns the connection: only the holder can make the pool full.
    wait_holding "${holder}" 60 || echo "the holder never took the connection, so the pool was never full"
}

# The holder is killed only after the victim has finished, so the pool was full for the whole of the
# victim's attempt. It still holding here is what says so: a holder that had released early, or that
# had lost the connection to the victim and queued behind it, would not be.
function release_pool() {
    local holder="${PREFIX}_${1}_holder"
    [[ $(holding "${holder}") != 0 ]] || echo "the holder was not holding the connection, so the pool was not full throughout"
    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${holder}' ASYNC" > /dev/null
    wait "${HOLDER_PID}" 2>/dev/null || true
}

# Arm A: two shards, each holding the whole table, and the first shard's only replica has no free
# connection slot. Complete is '90 20' and the defect is a silent '45 10'; the fix reports
# ALL_CONNECTION_TRIES_FAILED instead, the same code skip_unavailable_shards = 0 already produces.
# Printing the rows on the failing path is what separates the defect from any other failure: the
# defect is a result, not an error.
function arm_a() {
    local hedged=$1 label="a${1}"
    local out rc=0

    hold_pool "${label}"
    out=$(timeout 60 ${CLICKHOUSE_CLIENT} --query_id "${PREFIX}_${label}_victim" --query "
        SELECT sum(x), count() FROM remote('${ADDR},${ALT}', '${CLICKHOUSE_DATABASE}', 't', '${USER}', '')
        SETTINGS ${COMMON}, connection_pool_max_wait_ms = 300, skip_unavailable_shards = 1,
                 skip_unavailable_shards_mode = 'unavailable', use_hedged_requests = ${hedged}
    " 2>/dev/null) || rc=$?
    release_pool "${label}"

    echo "arm A, use_hedged_requests = ${hedged}"
    [[ ${rc} != 0 ]] || echo "FAIL: the query succeeded and returned [${out}]"
}

# Arm B: one shard with two replicas, the first of which has no free connection slot. The soft
# per-replica classification is what makes the second replica serve the shard, and this arm is what
# says the fix did not trade the silent partial result for a query that fails while a healthy replica
# could answer. It passes before and after the fix.
function arm_b() {
    local hedged=$1 label="b${1}"
    local out rc=0

    hold_pool "${label}"
    out=$(timeout 60 ${CLICKHOUSE_CLIENT} --query_id "${PREFIX}_${label}_victim" --query "
        SELECT sum(x), count() FROM remote('${ADDR}|${ALT}', '${CLICKHOUSE_DATABASE}', 't', '${USER}', '')
        SETTINGS ${COMMON}, connection_pool_max_wait_ms = 300, skip_unavailable_shards = 1,
                 skip_unavailable_shards_mode = 'unavailable', use_hedged_requests = ${hedged},
                 load_balancing = 'in_order'
    " 2>/dev/null) || rc=$?
    release_pool "${label}"

    echo "arm B, use_hedged_requests = ${hedged}"
    echo "failover still serves the shard: ${out}"
}

function insert_select() {
    timeout 60 ${CLICKHOUSE_CLIENT} --query_id "${1}" --query "
        INSERT INTO FUNCTION remote('${ADDR},${ALT}', '${CLICKHOUSE_DATABASE}', 'dst', '${USER}', '')
        SELECT * FROM remote('${ADDR},${ALT}', '${CLICKHOUSE_DATABASE}', 't', '${USER}', '')
        SETTINGS ${COMMON}, parallel_distributed_insert_select = 2, connection_pool_max_wait_ms = 300,
                 skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable',
                 use_hedged_requests = 0
    " > /dev/null 2>&1
}

# dst is the one MergeTree table this test reads directly, so it is where the runner's randomized
# parallel replicas would otherwise engage: this count is answered from part metadata only on the runs
# where optimize_trivial_count_query, itself randomized, lands on.
function landed_rows() {
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM dst SETTINGS enable_parallel_replicas = 0"
}

# Arm C: the same missing shard on the write path. parallel_distributed_insert_select sends the whole
# INSERT SELECT to each shard of the destination, and a shard the initiator had no connection slot for
# was dropped from the write while the INSERT still reported success. Complete is 20 rows, the defect
# is 10 with exit code 0, and the fix reports 279 and stores none. The uncontended control runs first,
# so the contended count is read against a measured complete value rather than an assumed one: an arm
# whose insert wrote nothing either way would otherwise look like a pass.
# use_hedged_requests is not an axis here, unlike in arms A and B: this path takes its connections from
# ConnectionPoolWithFailover::getMany and passes them to RemoteQueryExecutor, so the hedged factory is
# never the allocator.
function arm_c() {
    local rc=0 control landed

    ${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE dst"
    insert_select "${PREFIX}_c0_control"
    control=$(landed_rows)

    ${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE dst"
    hold_pool "c0"
    insert_select "${PREFIX}_c0_victim" || rc=$?
    release_pool "c0"
    landed=$(landed_rows)

    echo "arm C, parallel_distributed_insert_select = 2"
    echo "every shard is written when no pool is full: ${control}"
    [[ ${rc} != 0 ]] || echo "FAIL: the insert succeeded and landed ${landed} rows"
    echo "rows landed: ${landed}"
}

# Arm D: a shard that really is unavailable is still skipped silently. This is the feature working as
# documented, and it is what a fix that just raised the minimum entry count would break.
function arm_d() {
    local out rc=0
    out=$(timeout 60 ${CLICKHOUSE_CLIENT} --query_id "${PREFIX}_d_victim" --query "
        SELECT sum(x), count() FROM remote('${ADDR},127.0.0.1:1', '${CLICKHOUSE_DATABASE}', 't', '${USER}', '')
        SETTINGS ${COMMON}, connection_pool_max_wait_ms = 300, skip_unavailable_shards = 1,
                 skip_unavailable_shards_mode = 'unavailable', use_hedged_requests = 1
    " 2>/dev/null) || rc=$?

    echo "arm D, a genuinely unavailable shard is still skipped"
    echo "live shard still answers: ${out}"
}

arm_a 0
arm_a 1
arm_b 0
arm_b 1
arm_c
arm_d

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# Each arm that contends has to have waited on a full pool, or it measured nothing:
# ConnectionPoolIsFullMicroseconds says the victim really did reach an exhausted pool, and unlike
# system.processes it is still readable after the query is over. DistributedShardsSkipped separates
# the two verdicts that both end in missing rows: arm A must report the failure rather than record a
# skipped shard, and arm D must still record one.
# system.query_log is a MergeTree family table, so this read is the other place the runner's randomized
# parallel replicas would engage.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        splitByChar('_', query_id)[-2] AS arm,
        exception_code,
        position(exception, 'NO_FREE_CONNECTION') > 0 AS reason_is_the_pool_timeout,
        ProfileEvents['ConnectionPoolIsFullMicroseconds'] > 0 AS waited_on_a_full_pool,
        ProfileEvents['DistributedConnectionFailTry'] >= 1 AS a_replica_was_tried_and_failed,
        ProfileEvents['DistributedShardsSkipped'] AS shards_skipped
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
      AND query_id LIKE '${PREFIX}\_%\_victim' AND type != 'QueryStart' AND is_initial_query
    ORDER BY arm
    SETTINGS enable_parallel_replicas = 0
    FORMAT TSVWithNames
"

${CLICKHOUSE_CLIENT} --query "DROP USER ${USER}"

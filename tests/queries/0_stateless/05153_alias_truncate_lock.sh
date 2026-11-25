#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, memory-engine, log-engine
# no-replicated-database: there TRUNCATE is enqueued as a DDL entry instead of taking the lock these
#                         arms measure, so every arm would report blocked=0 regardless of the fix.
# no-shared-merge-tree, memory-engine, log-engine: the fixture assertion pins each engine name, and the
#                         ReplicatedMergeTree -> SharedMergeTree, Memory -> MergeTree and
#                         Log -> MergeTree test-runner replacements rewrite three of them.
#
# TRUNCATE through an Alias must choose its lock on, and take it on, the storage that executes the
# truncate. Each arm starts a reader, then issues TRUNCATE with a 3s lock_acquire_timeout and reports
# whether that reader blocked it. Arms I and J repeat A and B, and arm K repeats G, with the target in
# a `lazy_load_tables` database, where the catalog answers with a stand-in until the target is first
# accessed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE mt_a  (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE rmt_b (id UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt_b', 'r1') ORDER BY id;
    CREATE TABLE mem_c (id UInt64) ENGINE = Memory;
    CREATE TABLE mt_d  (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE mt_e  (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE mem_g (id UInt64) ENGINE = Memory;
    CREATE TABLE mt_h  (id UInt64) ENGINE = MergeTree ORDER BY id;

    INSERT INTO mt_a  SELECT number FROM numbers(300);
    INSERT INTO rmt_b SELECT number FROM numbers(300);
    INSERT INTO mem_c SELECT number FROM numbers(300);
    INSERT INTO mt_d  SELECT number FROM numbers(300);
    INSERT INTO mt_e  SELECT number FROM numbers(300);
    INSERT INTO mem_g SELECT number FROM numbers(300);
    INSERT INTO mt_h  SELECT number FROM numbers(300);
"

# al_d_outer is created while al_d_inner is still absent: an Alias whose target already resolves to
# an Alias is rejected at creation, an Alias to a missing name is not.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE al_a       ENGINE = Alias(currentDatabase(), 'mt_a');
    CREATE TABLE al_b       ENGINE = Alias(currentDatabase(), 'rmt_b');
    CREATE TABLE al_c       ENGINE = Alias(currentDatabase(), 'mem_c');
    CREATE TABLE al_d_outer ENGINE = Alias(currentDatabase(), 'al_d_inner');
    CREATE TABLE al_d_inner ENGINE = Alias(currentDatabase(), 'mt_d');
    CREATE TABLE al_f       ENGINE = Alias(currentDatabase(), 'no_such_table');
    CREATE TABLE al_g       ENGINE = Alias(currentDatabase(), 'mem_g');
    CREATE TABLE al_h       ENGINE = Alias(currentDatabase(), 'mt_h');
"

LAZY_DB="${CLICKHOUSE_DATABASE}_lazy"
cleanup() { $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${LAZY_DB} SYNC"; }
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE ${LAZY_DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

    CREATE TABLE ${LAZY_DB}.mt_i  (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${LAZY_DB}.rmt_j (id UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt_j', 'r1') ORDER BY id;
    CREATE TABLE ${LAZY_DB}.log_k (id UInt64) ENGINE = Log;

    INSERT INTO ${LAZY_DB}.mt_i  SELECT number FROM numbers(300);
    INSERT INTO ${LAZY_DB}.rmt_j SELECT number FROM numbers(300);
    INSERT INTO ${LAZY_DB}.log_k SELECT number FROM numbers(300);
"

# The inserts materialized all three tables; re-attaching the DATABASE is what puts the stand-ins back.
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${LAZY_DB} SYNC"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${LAZY_DB}"

# Created after the re-attach, so the targets are still stand-ins: creating an Alias does not read its
# target, so it does not load it either. The assertion below is what checks that.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE al_i ENGINE = Alias('${LAZY_DB}', 'mt_i');
    CREATE TABLE al_j ENGINE = Alias('${LAZY_DB}', 'rmt_j');
    CREATE TABLE al_k ENGINE = Alias('${LAZY_DB}', 'log_k');
"

# Without this an arm could report the expected value while probing a plain table instead of an alias.
echo '-- fixture'
$CLICKHOUSE_CLIENT -q "SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name"

# `TableProxy` is what makes arms I/J/K lazy-target arms; a `MergeTree`/`Log` here instead would mean
# the targets were already materialized and those arms would pass without exercising the lazy path.
# Only `name` and `engine` are selected: `parts` and `data_paths` would materialize the stand-ins.
echo '-- fixture lazy targets'
$CLICKHOUSE_CLIENT -q "SELECT name, engine FROM system.tables WHERE database = '${LAZY_DB}' ORDER BY name"

# arm <label> <reader table> <truncate target> <count table>
arm() {
    local label="$1" reader_table="$2" truncate_target="$3" count_table="$4"
    local query_id="${CLICKHOUSE_DATABASE}_${label}"

    $CLICKHOUSE_CLIENT --query_id "${query_id}" -q "
        SELECT sum(sleepEachRow(0.2)) FROM ${reader_table}
        SETTINGS max_block_size = 1, max_threads = 1,
                 function_sleep_max_microseconds_per_block = 100000000
    " > /dev/null 2>&1 &
    local reader_pid=$!

    # read_rows > 0 means the storage read has begun, which happens after the reader's share lock is
    # taken, so this handshake cannot report a reader that does not yet hold the lock.
    local started=0
    for _ in {1..200}; do
        if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '${query_id}' AND read_rows > 0")" = "1" ]; then
            started=1
            break
        fi
        sleep 0.1
    done
    if [ "${started}" != "1" ]; then
        echo "${label} ERROR reader did not start"
        $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '${query_id}' SYNC" > /dev/null 2>&1
        wait "${reader_pid}" 2>/dev/null
        return
    fi

    local err
    err=$($CLICKHOUSE_CLIENT -q "TRUNCATE TABLE ${truncate_target} SETTINGS lock_acquire_timeout = 3" 2>&1)
    local blocked=0
    if echo "${err}" | grep -q "WRITE locking attempt on .* has timed out.*Possible deadlock avoided"; then
        blocked=1
    elif [ -n "${err}" ]; then
        echo "${label} UNEXPECTED $(echo "${err}" | head -1)"
    fi

    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '${query_id}' SYNC" > /dev/null 2>&1
    wait "${reader_pid}" 2>/dev/null

    echo "${label} blocked=${blocked} rows_after=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${count_table}")"
}

echo '-- arms'
#   label                                reader      truncate    count
arm A_alias_to_mergetree                 al_a        al_a        mt_a
arm B_alias_to_replicated_mergetree      al_b        al_b        rmt_b
arm C_alias_to_memory                    al_c        al_c        mem_c
arm D_alias_chain_to_mergetree           al_d_outer  al_d_outer  mt_d
arm E_direct_mergetree                   mt_e        mt_e        mt_e
arm G_direct_reader_of_memory_target     mem_g       al_g        mem_g
arm H_direct_reader_of_mergetree_target  mt_h        al_h        mt_h
# I/J are A/B with a lazily loaded target. K is the control, and repeats G: its reader sits on the
# target itself, so nothing holds the alias and the only lock left that can time out is the target's,
# which is the one TRUNCATE picks once it has resolved the alias. A non-MergeTree target must keep
# excluding its readers, so an exemption that fired for every stand-in would show up here.
arm I_alias_to_lazy_mergetree            al_i        al_i        "${LAZY_DB}".mt_i
arm J_alias_to_lazy_replicated_mergetree al_j        al_j        "${LAZY_DB}".rmt_j
arm K_direct_reader_of_lazy_log_target   "${LAZY_DB}".log_k  al_k  "${LAZY_DB}".log_k

# F has no reader: an alias whose target cannot be resolved must still fail to resolve, not time out
# waiting for a lock.
F_ERR=$($CLICKHOUSE_CLIENT -q "TRUNCATE TABLE al_f SETTINGS lock_acquire_timeout = 3" 2>&1)
if echo "$F_ERR" | grep -q "UNKNOWN_TABLE"; then
    echo "F_dangling_alias err=UNKNOWN_TABLE"
else
    echo "F_dangling_alias err=UNEXPECTED $(echo "$F_ERR" | head -1)"
fi

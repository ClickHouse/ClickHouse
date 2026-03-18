#!/usr/bin/env bash
# Tags: long, no-parallel, disabled
# Tag: no-parallel - too heavy
# Tag: long        - too heavy
# Tag: disabled    - Always takes 4+ minutes, in serial mode, which is too much to be always run in CI

# This is the regression test when remote peer send some logs for INSERT,
# it is easy to archive using materialized views, with small block size.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=trace

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# NOTE: that since we use CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL we need to apply
# --server_logs_file for every clickhouse-client invocation.
client_opts=(
    # For --send_logs_level see $CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL
    --server_logs_file /dev/null
    # we need lots of blocks to get log entry for each of them
    --min_insert_block_size_rows 1
    # we need to terminate ASAP
    --max_block_size 1
)

$CLICKHOUSE_CLIENT "${client_opts[@]}" -nm -q "
    drop table if exists mv_02232;
    drop table if exists in_02232;
    drop table if exists out_02232;

    create table out_02232 (key Int) engine=Null();
    create table in_02232 (key Int) engine=Null();
    create materialized view mv_02232 to out_02232 as select * from in_02232;
"

# 600 is the default timeout of clickhouse-test, and 30 is just a safe padding,
# to avoid hung query check triggering
insert_timeout=$((600-30))
# Increase timeouts to avoid timeout during trying to send Log packet to
# the remote side, when the socket is full.
insert_client_opts=(
    --send_timeout "$insert_timeout"
    --receive_timeout "$insert_timeout"
)
# 250 seconds is enough to trigger the query hung (even in debug build)
#
# NOTE: using proper termination (via SIGINT) is too long,
# hence timeout+KILL QUERY.
timeout 250s $CLICKHOUSE_CLIENT "${client_opts[@]}" "${insert_client_opts[@]}" -q "insert into function remote('127.2', currentDatabase(), in_02232) select * from numbers(1e6)"

# Kill underlying query of remote() to make KILL faster
# This test is reproducing very interesting behaviour.
# The block size is 1, so the secondary query creates InterpreterSelectQuery for each row due to pushing to the MV.
# It works extremely slow, and the initial query produces new blocks and writes them to the socket much faster
# than the secondary query can read and process them. Therefore, it fills network buffers in the kernel.
# Once a buffer in the kernel is full, send(...) blocks until the secondary query will finish processing data
# that it already has in ReadBufferFromPocoSocket and call recv.
# Or until the kernel will decide to resize the buffer (seems like it has non-trivial rules for that).
# Anyway, it may look like the initial query got stuck, but actually it did not.
# Moreover, the initial query cannot be killed at that point, so KILL QUERY ... SYNC will get "stuck" as well.
timeout 30s $CLICKHOUSE_CLIENT "${client_opts[@]}" -q "KILL QUERY WHERE query like '%INSERT INTO $CLICKHOUSE_DATABASE.in_02232%' SYNC" --format Null
echo $?

$CLICKHOUSE_CLIENT "${client_opts[@]}" -nm -q "
    drop table in_02232;
    drop table mv_02232;
    drop table out_02232;
"

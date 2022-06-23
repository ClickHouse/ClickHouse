#!/usr/bin/env bash
# Tags: disabled
# Tag: no-parallel - to heavy
# Tag: long        - to heavy

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

insert_client_opts=(
    # Increase timeouts to avoid timeout during trying to send Log packet to
    # the remote side, when the socket is full.
    --send_timeout 86400
    --receive_timeout 86400
)
# 250 seconds is enough to trigger the query hung (even in debug build)
#
# NOTE: using proper termination (via SIGINT) is too long,
# hence timeout+KILL QUERY.
timeout 250s $CLICKHOUSE_CLIENT "${client_opts[@]}" "${insert_client_opts[@]}" -q "insert into function remote('127.2', currentDatabase(), in_02232) select * from numbers(1e6)"

# Kill underlying query of remote() to make KILL faster
timeout 30s $CLICKHOUSE_CLIENT "${client_opts[@]}" -q "KILL QUERY WHERE Settings['log_comment'] = '$CLICKHOUSE_LOG_COMMENT' SYNC" --format Null
echo $?

$CLICKHOUSE_CLIENT "${client_opts[@]}" -nm -q "
    drop table in_02232;
    drop table mv_02232;
    drop table out_02232;
"

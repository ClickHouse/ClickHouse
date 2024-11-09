#!/bin/bash

# shellcheck source=./utils.lib
source /repo/tests/docker_scripts/utils.lib

function attach_gdb_to_clickhouse()
{
    IS_ASAN=$(clickhouse-client --query "SELECT count() FROM system.build_options WHERE name = 'CXX_FLAGS' AND position('sanitize=address' IN value)")
    if [[ "$IS_ASAN" = "1" ]];
    then
        echo "ASAN build detected. Not using gdb since it disables LeakSanitizer detections"
    else
            # Set follow-fork-mode to parent, because we attach to clickhouse-server, not to watchdog
            # and clickhouse-server can do fork-exec, for example, to run some bridge.
            # Do not set nostop noprint for all signals, because some it may cause gdb to hang,
            # explicitly ignore non-fatal signals that are used by server.
            # Number of SIGRTMIN can be determined only in runtime.
            RTMIN=$(kill -l SIGRTMIN)
            # shellcheck disable=SC2016
            echo "
        set follow-fork-mode parent
        handle SIGHUP nostop noprint pass
        handle SIGINT nostop noprint pass
        handle SIGQUIT nostop noprint pass
        handle SIGPIPE nostop noprint pass
        handle SIGTERM nostop noprint pass
        handle SIGUSR1 nostop noprint pass
        handle SIGUSR2 nostop noprint pass
        handle SIG$RTMIN nostop noprint pass
        info signals
        continue
        backtrace full
        info registers
        p "top 1 KiB of the stack:"
        p/x *(uint64_t[128]*)"'$sp'"
        maintenance info sections
        thread apply all backtrace full
        disassemble /s
        up
        disassemble /s
        up
        disassemble /s
        p \"done\"
        detach
        quit
        " > script.gdb

            # FIXME Hung check may work incorrectly because of attached gdb
            # We cannot attach another gdb to get stacktraces if some queries hung
            gdb -batch -command script.gdb -p "$(cat /var/run/clickhouse-server/clickhouse-server.pid)" | ts '%Y-%m-%d %H:%M:%S' >> /test_output/gdb.log &
            sleep 5
            # gdb will send SIGSTOP, spend some time loading debug info and then send SIGCONT, wait for it (up to send_timeout, 300s)
            run_with_retry 60 clickhouse-client --query "SELECT 'Connected to clickhouse-server after attaching gdb'"
    fi
}

# vi: ft=bash

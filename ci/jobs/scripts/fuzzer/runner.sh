#!/bin/bash

repo_dir=.
FUZZER_TO_RUN="AST Fuzzer"

function filter_exists_and_template
{
    local path
    for path in "$@"; do
        if [ -e "$path" ]; then
            # SC2001 shellcheck suggests:
            # echo ${path//.sql.j2/.gen.sql}
            # but it doesn't allow to use regex
            echo "$path" | sed 's/\.sql\.j2$/.gen.sql/'
        else
            echo "'$path' does not exist" >&2
        fi
    done
}


function fuzz
{
    python3 $repo_dir/ci/jobs/scripts/fuzzer/generate-test-j2.py --path $repo_dir/tests/queries/0_stateless

    # Obtain the list of newly added tests. They will be fuzzed in more extreme way than other tests.
    # Don't overwrite the NEW_TESTS_OPT so that it can be set from the environment.
    NEW_TESTS="$(sed -n 's!\(^tests/queries/0_stateless/.*\.sql\(\.j2\)\?\)$!ch/\1!p' $repo_dir/ci-changed-files.txt | sort -R)"
    # ci-changed-files.txt contains also files that has been deleted/renamed, filter them out.
    NEW_TESTS="$(filter_exists_and_template $NEW_TESTS)"
    if [[ -n "$NEW_TESTS" ]]
    then
        NEW_TESTS_OPT="${NEW_TESTS_OPT:---interleave-queries-file ${NEW_TESTS}}"
    else
        NEW_TESTS_OPT="${NEW_TESTS_OPT:-}"
    fi

    # Setup arguments for the fuzzer
    FUZZER_OUTPUT_SQL_FILE=''

    if [[ "$FUZZER_TO_RUN" = "AST Fuzzer" ]];
    then
        QUERIES_FILE=$(find $repo_dir/tests/queries/0_stateless -type f -name "*.sql" | sort -R)
        FUZZER_ARGS="--query-fuzzer-runs=1000 --create-query-fuzzer-runs=50 --queries-file $QUERIES_FILE $NEW_TESTS_OPT"
    elif [ "$FUZZER_TO_RUN" = "BuzzHouse" ]
    then
        touch fuzzer_out.sql fuzz.json
        FUZZER_OUTPUT_SQL_FILE=$(realpath fuzzer_out.sql)
        BUZZHOUSE_CONFIG_FILE=$(realpath fuzz.json)
cat << EOF > $BUZZHOUSE_CONFIG_FILE
{
    "db_file_path": "/var/lib/clickhouse/user_files",
    "log_path": "$FUZZER_OUTPUT_SQL_FILE",
    "seed": 0,
    "read_log": false,
    "use_dump_table_oracle": false,
    "time_to_run": 180
}
EOF
        FUZZER_ARGS="--buzz-house-config=$BUZZHOUSE_CONFIG_FILE"
    else
        >&2 echo "Fuzzer \"$FUZZER_TO_RUN\" unknown, provide either \"AST Fuzzer\" or \"BuzzHouse\""
        exit 1
    fi

    # Allow the fuzzer to run for some time, giving it a grace period of 5m to finish once the time
    # out triggers. After that, it'll send a SIGKILL to the fuzzer to make sure it finishes within
    # a reasonable time.
    timeout -s TERM --kill-after=5m --preserve-status 30m clickhouse-client \
        --max_memory_usage_in_client=1000000000 \
        --receive_timeout=10 \
        --receive_data_timeout_ms=10000 \
        --stacktrace \
        $FUZZER_ARGS \
        > fuzzer.log \
        2>&1 &
    fuzzer_pid=$!
    echo "Fuzzer pid is $fuzzer_pid"

    # We need to give timeout some time to execute the underlying command with that many arguments
    elapsed=0
    maximum=50
    while [[ $elapsed -lt $maximum ]]; do
        if ps -o pid= --ppid "$fuzzer_pid"; then
            echo "Found underlying PID!"
            break;
        else
            echo "Not found. Trying again..."
        fi
        sleep 0.1
        elapsed=$((elapsed+1))
    done

    # The fuzzer_pid belongs to the timeout process.
    actual_fuzzer_pid=$(ps -o pid= --ppid "$fuzzer_pid")

    if [[ "$IS_ASAN" = "1" ]];
    then
        echo "ASAN build detected. Not using gdb since it disables LeakSanitizer detections"
    else
        echo "Attaching gdb to the fuzzer itself"
        gdb -batch -command script.gdb -p $actual_fuzzer_pid &
    fi

    # Wait for the fuzzer to complete.
    # Note that the 'wait || ...' thing is required so that the script doesn't
    # exit because of 'set -e' when 'wait' returns nonzero code.
    fuzzer_exit_code=0
    wait "$fuzzer_pid" || fuzzer_exit_code=$?
    echo "Fuzzer exit code is $fuzzer_exit_code"
}

fuzz
exit $fuzzer_exit_code

#    # If the server dies, most often the fuzzer returns Code 210: Connetion
#    # refused, and sometimes also code 32: attempt to read after eof. For
#    # simplicity, check again whether the server is accepting connections using
#    # clickhouse-client. We don't check for the existence of the server process, because
#    # the process is still present while the server is terminating and not
#    # accepting the connections anymore.
#
#    for _ in {1..100}
#    do
#        if clickhouse-client --query "SELECT 1" 2> err
#        then
#            server_died=0
#            break
#        else
#            # There are legitimate queries leading to this error, example:
#            # SELECT * FROM remote('127.0.0.{1..255}', system, one)
#            if grep -F 'TOO_MANY_SIMULTANEOUS_QUERIES' err
#            then
#                # Give it some time to cool down
#                clickhouse-client --query "SHOW PROCESSLIST"
#                sleep 1
#            else
#                echo "Server live check returns $?"
#                cat err
#                server_died=1
#                break
#            fi
#        fi
#    done
#
#
#
#    # wait in background to call wait in foreground and ensure that the
#    # process is alive, since w/o job control this is the only way to obtain
#    # the exit code
#    stop_server &
#    server_exit_code=0
#    wait $server_pid || server_exit_code=$?
#    echo "Server exit code is $server_exit_code"
#
#    # Make files with status and description we'll show for this check on Github.
#    task_exit_code=$fuzzer_exit_code
#    if [ "$FUZZER_TO_RUN" = "BuzzHouse" ]
#    then
#        echo "BuzzHouse may fail for now. Please inspect the log to find the issues it found."
#
#        task_exit_code=0
#        echo "success" > status.txt
#        echo "OK" > description.txt
#    elif [ "$server_died" == 1 ]
#    then
#        # The server has died.
#        if ! rg --text -o 'Received signal.*|Logical error.*|Assertion.*failed|Failed assertion.*|.*runtime error: .*|.*is located.*|(SUMMARY|ERROR): [a-zA-Z]+Sanitizer:.*|.*_LIBCPP_ASSERT.*|.*Child process was terminated by signal 9.*' server.log > description.txt
#        then
#            echo "Lost connection to server. See the logs." > description.txt
#        fi
#
#        IS_SANITIZED=$(clickhouse-local --query "SELECT value LIKE '%-fsanitize=%' FROM system.build_options WHERE name = 'CXX_FLAGS'")
#
#        if [ "${IS_SANITIZED}" -eq "1" ] && rg --text 'Sanitizer:? (out-of-memory|out of memory|failed to allocate)|Child process was terminated by signal 9' description.txt
#        then
#            # OOM of sanitizer is not a problem we can handle - treat it as success, but preserve the description.
#            # Why? Because sanitizers have the memory overhead, that is not controllable from inside clickhouse-server.
#            task_exit_code=0
#            echo "success" > status.txt
#        else
#            task_exit_code=210
#            echo "failure" > status.txt
#        fi
#    elif [ "$fuzzer_exit_code" == "143" ] || [ "$fuzzer_exit_code" == "0" ]
#    then
#        # Variants of a normal run:
#        # 0 -- fuzzing ended earlier than timeout.
#        # 143 -- SIGTERM -- the fuzzer was killed by timeout.
#        task_exit_code=0
#        echo "success" > status.txt
#        echo "OK" > description.txt
#    elif [ "$fuzzer_exit_code" == "137" ]
#    then
#        # Killed.
#        task_exit_code=$fuzzer_exit_code
#        echo "failure" > status.txt
#        echo "Killed" > description.txt
#    else
#        # The server was alive, but the fuzzer returned some error. This might
#        # be some client-side error detected by fuzzing, or a problem in the
#        # fuzzer itself. Don't grep the server log in this case, because we will
#        # find a message about normal server termination (Received signal 15),
#        # which is confusing.
#        task_exit_code=$fuzzer_exit_code
#        echo "failure" > status.txt
#        echo "Let op!" > description.txt
#        echo "Fuzzer went wrong with error code: ($fuzzer_exit_code). Its process died somehow when the server stayed alive. The server log probably won't tell you much so try to find information in other files." >>description.txt
#        { rg -ao "Found error:.*" fuzzer.log || rg -ao "Exception:.*" fuzzer.log; } | tail -1 >>description.txt
#    fi
#
#    if test -f core.*; then
#        zstd --threads=0 core.*
#        mv core.*.zst core.zst
#    fi
#
#    dmesg -T | rg -q -F -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE' && echo "OOM in dmesg" ||:
#}
#
#case "$stage" in
#"")
#    ;&  # Did you know? This is "fallthrough" in bash. https://stackoverflow.com/questions/12010686/case-statement-fallthrough
#"fuzz")
#    time fuzz
#    ;&
#esac
#
#dmesg -T > dmesg.log ||:
#
#zstd --threads=0 --rm server.log
#zstd --threads=0 --rm fuzzer.log
#
#if [ -f $FUZZER_OUTPUT_SQL_FILE ]; then
#    zstd --threads=0 --rm $FUZZER_OUTPUT_SQL_FILE
#fi
#
#exit $task_exit_code

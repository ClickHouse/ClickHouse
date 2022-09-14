#!/bin/bash
# shellcheck disable=SC2086,SC2001,SC2046,SC2030,SC2031

set -eux
set -o pipefail
trap "exit" INT TERM
# The watchdog is in the separate process group, so we have to kill it separately
# if the script terminates earlier.
trap 'kill $(jobs -pr) ${watchdog_pid:-} ||:' EXIT

stage=${stage:-}
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "$script_dir"
repo_dir=ch
BINARY_TO_DOWNLOAD=${BINARY_TO_DOWNLOAD:="clang-13_debug_none_bundled_unsplitted_disable_False_binary"}
BINARY_URL_TO_DOWNLOAD=${BINARY_URL_TO_DOWNLOAD:="https://clickhouse-builds.s3.amazonaws.com/$PR_TO_TEST/$SHA_TO_TEST/clickhouse_build_check/$BINARY_TO_DOWNLOAD/clickhouse"}

function clone
{
    # For local runs, start directly from the "fuzz" stage.
    rm -rf "$repo_dir" ||:
    mkdir "$repo_dir" ||:

    git clone --depth 1 https://github.com/ClickHouse/ClickHouse.git -- "$repo_dir" 2>&1 | ts '%Y-%m-%d %H:%M:%S'
    (
        cd "$repo_dir"
        if [ "$PR_TO_TEST" != "0" ]; then
            if git fetch --depth 1 origin "+refs/pull/$PR_TO_TEST/merge"; then
                git checkout FETCH_HEAD
                echo "Checked out pull/$PR_TO_TEST/merge ($(git rev-parse FETCH_HEAD))"
            else
                git fetch --depth 1 origin "+refs/pull/$PR_TO_TEST/head"
                git checkout "$SHA_TO_TEST"
                echo "Checked out nominal SHA $SHA_TO_TEST for PR $PR_TO_TEST"
            fi
            git diff --name-only master HEAD | tee ci-changed-files.txt
        else
            if [ -v SHA_TO_TEST ]; then
                git fetch --depth 2 origin "$SHA_TO_TEST"
                git checkout "$SHA_TO_TEST"
                echo "Checked out nominal SHA $SHA_TO_TEST for master"
            else
                git fetch --depth 2 origin
                echo "Using default repository head $(git rev-parse HEAD)"
            fi
            git diff --name-only HEAD~1 HEAD | tee ci-changed-files.txt
        fi
        cd -
    )

    ls -lath ||:

}

function wget_with_retry
{
    for _ in 1 2 3 4; do
        if wget -nv -nd -c "$1";then
            return 0
        else
            sleep 0.5
        fi
    done
    return 1
}

function download
{
    wget_with_retry "$BINARY_URL_TO_DOWNLOAD"

    chmod +x clickhouse
    ln -s ./clickhouse ./clickhouse-server
    ln -s ./clickhouse ./clickhouse-client

    # clickhouse-server is in the current dir
    export PATH="$PWD:$PATH"
}

function configure
{
    rm -rf db ||:
    mkdir db ||:
    cp -av --dereference "$repo_dir"/programs/server/config* db
    cp -av --dereference "$repo_dir"/programs/server/user* db
    # TODO figure out which ones are needed
    cp -av --dereference "$repo_dir"/tests/config/config.d/listen.xml db/config.d
    cp -av --dereference "$script_dir"/query-fuzzer-tweaks-users.xml db/users.d
}

function watchdog
{
    sleep 1800

    echo "Fuzzing run has timed out"
    for _ in {1..10}
    do
        # Only kill by pid the particular client that runs the fuzzing, or else
        # we can kill some clickhouse-client processes this script starts later,
        # e.g. for checking server liveness.
        if ! kill $fuzzer_pid
        then
            break
        fi
        sleep 1
    done

    kill -9 -- $fuzzer_pid ||:
}

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
            echo "'$path' does not exists" >&2
        fi
    done
}

function stop_server
{
    clickhouse-client --query "select elapsed, query from system.processes" ||:
    killall clickhouse-server ||:
    for _ in {1..10}
    do
        if ! pgrep -f clickhouse-server
        then
            break
        fi
        sleep 1
    done
    killall -9 clickhouse-server ||:

    # Debug.
    date
    sleep 10
    jobs
    pstree -aspgT
}

function fuzz
{
    /generate-test-j2.py --path ch/tests/queries/0_stateless

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

    # interferes with gdb
    export CLICKHOUSE_WATCHDOG_ENABLE=0
    # NOTE: we use process substitution here to preserve keep $! as a pid of clickhouse-server
    clickhouse-server --config-file db/config.xml -- --path db > >(tail -100000 > server.log) 2>&1 &
    server_pid=$!

    kill -0 $server_pid

    # Set follow-fork-mode to parent, because we attach to clickhouse-server, not to watchdog
    # and clickhouse-server can do fork-exec, for example, to run some bridge.
    # Do not set nostop noprint for all signals, because some it may cause gdb to hang,
    # explicitly ignore non-fatal signals that are used by server.
    # Number of SIGRTMIN can be determined only in runtime.
    RTMIN=$(kill -l SIGRTMIN)
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
gcore
backtrace full
thread apply all backtrace full
info registers
disassemble /s
up
disassemble /s
up
disassemble /s
p \"done\"
detach
quit
" > script.gdb

    gdb -batch -command script.gdb -p $server_pid  &
    sleep 5
    # gdb will send SIGSTOP, spend some time loading debug info and then send SIGCONT, wait for it (up to send_timeout, 300s)
    time clickhouse-client --query "SELECT 'Connected to clickhouse-server after attaching gdb'" ||:

    # Check connectivity after we attach gdb, because it might cause the server
    # to freeze and the fuzzer will fail. In debug build it can take a lot of time.
    for _ in {1..180}
    do
        sleep 1
        if clickhouse-client --query "select 1"
        then
            break
        fi
    done
    clickhouse-client --query "select 1" # This checks that the server is responding
    kill -0 $server_pid # This checks that it is our server that is started and not some other one
    echo Server started and responded

    # SC2012: Use find instead of ls to better handle non-alphanumeric filenames. They are all alphanumeric.
    # SC2046: Quote this to prevent word splitting. Actually I need word splitting.
    # shellcheck disable=SC2012,SC2046
    clickhouse-client \
        --receive_timeout=10 \
        --receive_data_timeout_ms=10000 \
        --stacktrace \
        --query-fuzzer-runs=1000 \
        --testmode \
        --queries-file $(ls -1 ch/tests/queries/0_stateless/*.sql | sort -R) \
        $NEW_TESTS_OPT \
        > >(tail -n 100000 > fuzzer.log) \
        2>&1 &
    fuzzer_pid=$!
    echo "Fuzzer pid is $fuzzer_pid"

    # Start a watchdog that should kill the fuzzer on timeout.
    # The shell won't kill the child sleep when we kill it, so we have to put it
    # into a separate process group so that we can kill them all.
    set -m
    watchdog &
    watchdog_pid=$!
    set +m
    # Check that the watchdog has started.
    kill -0 $watchdog_pid

    # Wait for the fuzzer to complete.
    # Note that the 'wait || ...' thing is required so that the script doesn't
    # exit because of 'set -e' when 'wait' returns nonzero code.
    fuzzer_exit_code=0
    wait "$fuzzer_pid" || fuzzer_exit_code=$?
    echo "Fuzzer exit code is $fuzzer_exit_code"

    kill -- -$watchdog_pid ||:

    # If the server dies, most often the fuzzer returns code 210: connetion
    # refused, and sometimes also code 32: attempt to read after eof. For
    # simplicity, check again whether the server is accepting connections, using
    # clickhouse-client. We don't check for existence of server process, because
    # the process is still present while the server is terminating and not
    # accepting the connections anymore.
    if clickhouse-client --query "select 1 format Null"
    then
        server_died=0
    else
        echo "Server live check returns $?"
        server_died=1
    fi

    # wait in background to call wait in foreground and ensure that the
    # process is alive, since w/o job control this is the only way to obtain
    # the exit code
    stop_server &
    server_exit_code=0
    wait $server_pid || server_exit_code=$?
    echo "Server exit code is $server_exit_code"

    # Make files with status and description we'll show for this check on Github.
    task_exit_code=$fuzzer_exit_code
    if [ "$server_died" == 1 ]
    then
        # The server has died.
        task_exit_code=210
        echo "failure" > status.txt
        if ! grep --text -ao "Received signal.*\|Logical error.*\|Assertion.*failed\|Failed assertion.*\|.*runtime error: .*\|.*is located.*\|SUMMARY: AddressSanitizer:.*\|SUMMARY: MemorySanitizer:.*\|SUMMARY: ThreadSanitizer:.*\|.*_LIBCPP_ASSERT.*" server.log > description.txt
        then
            echo "Lost connection to server. See the logs." > description.txt
        fi
    elif [ "$fuzzer_exit_code" == "143" ] || [ "$fuzzer_exit_code" == "0" ]
    then
        # Variants of a normal run:
        # 0 -- fuzzing ended earlier than timeout.
        # 143 -- SIGTERM -- the fuzzer was killed by timeout.
        task_exit_code=0
        echo "success" > status.txt
        echo "OK" > description.txt
    elif [ "$fuzzer_exit_code" == "137" ]
    then
        # Killed.
        task_exit_code=$fuzzer_exit_code
        echo "failure" > status.txt
        echo "Killed" > description.txt
    else
        # The server was alive, but the fuzzer returned some error. This might
        # be some client-side error detected by fuzzing, or a problem in the
        # fuzzer itself. Don't grep the server log in this case, because we will
        # find a message about normal server termination (Received signal 15),
        # which is confusing.
        task_exit_code=$fuzzer_exit_code
        echo "failure" > status.txt
        { grep --text -o "Found error:.*" fuzzer.log \
            || grep --text -ao "Exception:.*" fuzzer.log \
            || echo "Fuzzer failed ($fuzzer_exit_code). See the logs." ; } \
            | tail -1 > description.txt
    fi

    if test -f core.*; then
        pigz core.*
        mv core.*.gz core.gz
    fi
}

case "$stage" in
"")
    ;&  # Did you know? This is "fallthrough" in bash. https://stackoverflow.com/questions/12010686/case-statement-fallthrough
"clone")
    time clone
    if [ -v FUZZ_LOCAL_SCRIPT ]
    then
        # just fall through
        echo Using the testing script from docker container
        :
    else
        # Run the testing script from the repository
        echo Using the testing script from the repository
        export stage=download
        time ch/docker/test/fuzzer/run-fuzzer.sh
        # Keep the error code
        exit $?
    fi
    ;&
"download")
    time download
    ;&
"configure")
    time configure
    ;&
"fuzz")
    time fuzz
    ;&
"report")
CORE_LINK=''
if [ -f core.gz ]; then
    CORE_LINK='<a href="core.gz">core.gz</a>'
fi
cat > report.html <<EOF ||:
<!DOCTYPE html>
<html lang="en">
<link rel="preload" as="font" href="https://yastatic.net/adv-www/_/sUYVCPUAQE7ExrvMS7FoISoO83s.woff2" type="font/woff2" crossorigin="anonymous"/>
  <style>
@font-face {
    font-family:'Yandex Sans Display Web';
    src:url(https://yastatic.net/adv-www/_/H63jN0veW07XQUIA2317lr9UIm8.eot);
    src:url(https://yastatic.net/adv-www/_/H63jN0veW07XQUIA2317lr9UIm8.eot?#iefix) format('embedded-opentype'),
            url(https://yastatic.net/adv-www/_/sUYVCPUAQE7ExrvMS7FoISoO83s.woff2) format('woff2'),
            url(https://yastatic.net/adv-www/_/v2Sve_obH3rKm6rKrtSQpf-eB7U.woff) format('woff'),
            url(https://yastatic.net/adv-www/_/PzD8hWLMunow5i3RfJ6WQJAL7aI.ttf) format('truetype'),
            url(https://yastatic.net/adv-www/_/lF_KG5g4tpQNlYIgA0e77fBSZ5s.svg#YandexSansDisplayWeb-Regular) format('svg');
    font-weight:400;
    font-style:normal;
    font-stretch:normal
}

body { font-family: "Yandex Sans Display Web", Arial, sans-serif; background: #EEE; }
h1 { margin-left: 10px; }
th, td { border: 0; padding: 5px 10px 5px 10px; text-align: left; vertical-align: top; line-height: 1.5; background-color: #FFF;
td { white-space: pre; font-family: Monospace, Courier New; }
border: 0; box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.05), 0 8px 25px -5px rgba(0, 0, 0, 0.1); }
a { color: #06F; text-decoration: none; }
a:hover, a:active { color: #F40; text-decoration: underline; }
table { border: 0; }
.main { margin-left: 10%; }
p.links a { padding: 5px; margin: 3px; background: #FFF; line-height: 2; white-space: nowrap; box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.05), 0 8px 25px -5px rgba(0, 0, 0, 0.1); }
th { cursor: pointer; }

  </style>
  <title>AST Fuzzer for PR #${PR_TO_TEST} @ ${SHA_TO_TEST}</title>
</head>
<body>
<div class="main">

<h1>AST Fuzzer for PR #${PR_TO_TEST} @ ${SHA_TO_TEST}</h1>
<p class="links">
<a href="fuzzer.log">fuzzer.log</a>
<a href="server.log">server.log</a>
<a href="main.log">main.log</a>
${CORE_LINK}
</p>
<table>
<tr><th>Test name</th><th>Test status</th><th>Description</th></tr>
<tr><td>AST Fuzzer</td><td>$(cat status.txt)</td><td>$(cat description.txt)</td></tr>
</table>
</body>
</html>

EOF
    ;&
esac

exit $task_exit_code

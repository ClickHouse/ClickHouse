#!/bin/bash
# shellcheck disable=SC2086

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
BINARY_TO_DOWNLOAD=${BINARY_TO_DOWNLOAD:="clang-11_debug_none_bundled_unsplitted_disable_False_binary"}

function clone
{
    # The download() function is dependent on CI binaries anyway, so we can take
    # the repo from the CI as well. For local runs, start directly from the "fuzz"
    # stage.
    rm -rf ch ||:
    mkdir ch ||:
    wget -nv -nd -c "https://clickhouse-test-reports.s3.yandex.net/$PR_TO_TEST/$SHA_TO_TEST/repo/clickhouse_no_subs.tar.gz"
    tar -C ch --strip-components=1 -xf clickhouse_no_subs.tar.gz
    ls -lath ||:
}

function download
{
    wget -nv -nd -c "https://clickhouse-builds.s3.yandex.net/$PR_TO_TEST/$SHA_TO_TEST/clickhouse_build_check/$BINARY_TO_DOWNLOAD/clickhouse" &
    wget -nv -nd -c "https://clickhouse-test-reports.s3.yandex.net/$PR_TO_TEST/$SHA_TO_TEST/repo/ci-changed-files.txt" &
    wait

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
    sleep 3600

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

function filter_exists
{
    local path
    for path in "$@"; do
        if [ -e "$path" ]; then
            echo "$path"
        else
            echo "'$path' does not exists" >&2
        fi
    done
}

function fuzz
{
    # Obtain the list of newly added tests. They will be fuzzed in more extreme way than other tests.
    # Don't overwrite the NEW_TESTS_OPT so that it can be set from the environment.
    NEW_TESTS="$(sed -n 's!\(^tests/queries/0_stateless/.*\.sql\)$!ch/\1!p' ci-changed-files.txt | sort -R)"
    # ci-changed-files.txt contains also files that has been deleted/renamed, filter them out.
    NEW_TESTS="$(filter_exists $NEW_TESTS)"
    if [[ -n "$NEW_TESTS" ]]
    then
        NEW_TESTS_OPT="${NEW_TESTS_OPT:---interleave-queries-file ${NEW_TESTS}}"
    else
        NEW_TESTS_OPT="${NEW_TESTS_OPT:-}"
    fi

    export CLICKHOUSE_WATCHDOG_ENABLE=0 # interferes with gdb
    clickhouse-server --config-file db/config.xml -- --path db 2>&1 | tail -100000 > server.log &
    server_pid=$!
    kill -0 $server_pid

    echo "
set follow-fork-mode child
handle all noprint
handle SIGSEGV stop print
handle SIGBUS stop print
continue
thread apply all backtrace
continue
" > script.gdb

    gdb -batch -command script.gdb -p $server_pid &

    # Check connectivity after we attach gdb, because it might cause the server
    # to freeze and the fuzzer will fail.
    for _ in {1..60}
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
        --query-fuzzer-runs=1000 \
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

    # Stop the server.
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
    else
        # The server was alive, but the fuzzer returned some error. This might
        # be some client-side error detected by fuzzing, or a problem in the
        # fuzzer itself. Don't grep the server log in this case, because we will
        # find a message about normal server termination (Received signal 15),
        # which is confusing.
        task_exit_code=$fuzzer_exit_code
        echo "failure" > status.txt
        { grep --text -o "Found error:.*" fuzzer.log \
            || grep --text -o "Exception.*" fuzzer.log \
            || echo "Fuzzer failed ($fuzzer_exit_code). See the logs." ; } \
            | tail -1 > description.txt
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

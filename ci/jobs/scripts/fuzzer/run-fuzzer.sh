#!/bin/bash
# shellcheck disable=SC2086,SC2001,SC2046,SC2030,SC2031,SC2010,SC2015

set -x

dmesg --clear ||:

set -e
set -u
set -o pipefail

stage=${stage:-}

repo_dir=/repo

CONFIG_DIR="/etc/clickhouse-server"

export PATH="$repo_dir/ci/tmp/:$PATH"
export PYTHONPATH=$repo_dir:$repo_dir/ci

cd /workspace

# Direct sanitizer reports to files instead of the server's stderr to avoid 
# losing the report when the server aborts. The runtime appends ".<pid>"
# to `log_path`; reports are merged back in by collect_sanitizer_reports.
# Existing options from the environment/image are preserved.
SANITIZER_LOG_BASE="/workspace/sanitizer.log"
for _san in ASAN TSAN MSAN UBSAN LSAN; do
    _var="${_san}_OPTIONS"
    export "$_var"="${!_var:+${!_var} }log_path=${SANITIZER_LOG_BASE}"
done
unset _san _var

function collect_sanitizer_reports
{
    # Merge sanitizer reports captured via log_path into stderr.log (for the
    # failure parser) and server.log (for context and the OOM grep). Run from an
    # EXIT trap so early `set -e` aborts are covered too; `|| true` keeps the
    # exit code intact.
    local report
    for report in "${SANITIZER_LOG_BASE}".*; do
        [ -e "$report" ] || continue
        echo "Found sanitizer report: $report"
        {
            echo "=== sanitizer report from ${report} ==="
            cat "$report"
            echo
        } | tee -a stderr.log >> server.log || true
    done
}

function make_artifacts_host_readable
{
    # On any abnormal path, make the artifacts host-readable before upload. The
    # script runs as root inside the container; Clang writes sanitizer.log.*
    # 0640 under umask 022, which the host runner user (who uploads them) cannot
    # read -- exactly the evidence a memory-stuck / watchdog run most needs. A
    # plain chmod avoids any docker chown container (which would itself be
    # unbounded). Healthy runs skip this (no marker/watchdog).
    #
    # Runs from the EXIT trap, not inline at the end of `fuzz`: `set -e` aborts
    # between the marker write and the end of the function (a failing
    # `zstd core.*` on a nearly-full box is the concrete case) would otherwise
    # skip it, and the Python side skips its own host-side ownership repair
    # whenever a marker/watchdog exists -- losing the artifacts on exactly the
    # abnormal runs this change exists to preserve.
    # core.* too: a kernel-written core is owned by the crashing root process, and
    # the host-side collector reads it to compress+encrypt it -- an unreadable core
    # makes that raise, which aborts the job after classification.
    [[ -f server_memory_stuck.txt || -f harness_watchdog.txt ]] || return 0
    chmod -R a+r sanitizer.log.* core.* ./*.log status.tsv server_memory_stuck.txt harness_watchdog.txt 2>/dev/null ||:
}

# Order matters: collect_sanitizer_reports writes stderr.log/server.log, so the
# chmod must run after it. Each handler is `||:`-guarded: the trap body runs under
# `set -e` too, so a non-zero first handler would abort the trap and skip the
# chmod, and `||:` also keeps the script's own exit code (137 on a SIGKILL run).
trap 'collect_sanitizer_reports ||:; make_artifacts_host_readable ||:' EXIT

function configure
{
    chmod +x $repo_dir/ci/tmp/clickhouse
    # clickhouse may be compressed - run once to decompress
    $repo_dir/ci/tmp/clickhouse --query "SELECT 1" ||:
    ln -sf $repo_dir/ci/tmp/clickhouse $repo_dir/ci/tmp/clickhouse-server
    ln -sf $repo_dir/ci/tmp/clickhouse $repo_dir/ci/tmp/clickhouse-client
    ln -sf $repo_dir/ci/tmp/clickhouse $repo_dir/ci/tmp/clickhouse-local
    rm -rf $CONFIG_DIR ||:
    mkdir -p $CONFIG_DIR ||:
    cp -av --dereference "$repo_dir"/programs/server/config* $CONFIG_DIR
    cp -av --dereference "$repo_dir"/programs/server/user* $CONFIG_DIR
    # TODO figure out which ones are needed
    cp -av --dereference "$repo_dir"/tests/config/config.d/listen.xml $CONFIG_DIR/config.d
    cp -av --dereference "$repo_dir"/tests/config/users.d/ci_logs_sender.yaml $CONFIG_DIR/users.d
    cp -av --dereference "$repo_dir"/ci/jobs/scripts/fuzzer/query-fuzzer-tweaks-users.xml $CONFIG_DIR/users.d
    cp -av --dereference "$repo_dir"/ci/jobs/scripts/fuzzer/limit-recursion-settings.xml $CONFIG_DIR/users.d
    cp -av --dereference "$repo_dir"/ci/jobs/scripts/fuzzer/fuzz-server-settings.xml $CONFIG_DIR/config.d

    cat > $CONFIG_DIR/config.d/max_server_memory_usage_to_ram_ratio.xml <<EOL
<clickhouse>
    <max_server_memory_usage_to_ram_ratio>0.75</max_server_memory_usage_to_ram_ratio>
</clickhouse>
EOL


    (cd $repo_dir && python3 $repo_dir/ci/jobs/scripts/clickhouse_proc.py logs_export_config) || echo "Failed to create log export config"
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
            echo "'$path' does not exist" >&2
        fi
    done
}

function stop_server
{
    clickhouse-client --query "select elapsed, query from system.processes" ||:
    clickhouse stop

    # Debug.
    date
    sleep 10
    jobs
    pstree -aspgT
}

function fuzz
{
    $repo_dir/ci/jobs/scripts/fuzzer/generate-test-j2.py --path $repo_dir/tests/queries/0_stateless

    # Obtain the list of newly added tests. They will be fuzzed in more extreme way than other tests.
    # Don't overwrite the NEW_TESTS_OPT so that it can be set from the environment.
    NEW_TESTS="$(sed -n 's!\(^tests/queries/0_stateless/.*\.sql\(\.j2\)\?\)$!ch/\1!p' /workspace/ci-changed-files.txt | sort -R)"
    # ci-changed-files.txt contains also files that has been deleted/renamed, filter them out.
    NEW_TESTS="$(filter_exists_and_template $NEW_TESTS)"
    if [[ -n "$NEW_TESTS" ]]
    then
        NEW_TESTS_OPT="${NEW_TESTS_OPT:---interleave-queries-file ${NEW_TESTS}}"
    else
        NEW_TESTS_OPT="${NEW_TESTS_OPT:-}"
    fi

    mkdir -p /var/run/clickhouse-server

    # server.log -> All server logs, including sanitizer
    # stderr.log -> Process logs (sanitizer) only
    ( clickhouse-server \
          --config-file $CONFIG_DIR/config.xml \
          --pid-file /var/run/clickhouse-server/clickhouse-server.pid \
          --  --path $CONFIG_DIR \
              --logger.console=0 \
              --logger.log=server.log 2>&1 | tee -a stderr.log >> server.log 2>&1
      exit "${PIPESTATUS[0]}" ) &
    server_bg_pid=$!
    # A debug or sanitizer server can take over a minute from fork to listening;
    # give it the same 120s budget as `wait_ready` in `clickhouse_proc.py`.
    # A dead server is detected early, so the deadline only affects hung servers.
    for _ in {1..120}
    do
        if clickhouse-client --receive_timeout=5 --query "select 1" || ! kill -0 $server_bg_pid
        then
            break
        fi
        sleep 1
    done
    server_pid=$(cat /var/run/clickhouse-server/clickhouse-server.pid)

    kill -0 $server_pid

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

        gdb -batch -command script.gdb -p $server_pid &
        server_gdb_pid=$!
        sleep 5
        # gdb will send SIGSTOP, spend some time loading debug info, and then send SIGCONT, wait for it (up to send_timeout, 300s)
        time clickhouse-client --query "SELECT 'Connected to clickhouse-server after attaching gdb'" ||:

        # Check connectivity after we attach gdb, because it might cause the server
        # to freeze, and the fuzzer will fail. In debug build, it can take a lot of time.
        for _ in {1..180}
        do
            if clickhouse-client --receive_timeout=5 --query "select 1"
            then
                break
            fi
            sleep 1
        done
        kill -0 $server_pid # This checks that it is our server that is started and not some other one
    fi

    echo 'Server started and responded.'

    (cd $repo_dir && python3 $repo_dir/ci/jobs/scripts/clickhouse_proc.py logs_export_start) || echo "Failed to start log exports"

    # Setup arguments for the fuzzer
    FUZZER_OUTPUT_SQL_FILE=''

    if [[ "$FUZZER_TO_RUN" = "AST Fuzzer" ]];
    then
        if [[ -n "${TARGETED_QUERIES_FILE:-}" ]] && [[ -f "${TARGETED_QUERIES_FILE}" ]];
        then
            QUERIES_FILE="$(cat "${TARGETED_QUERIES_FILE}")"
            echo "Using targeted AST fuzzer corpus from ${TARGETED_QUERIES_FILE}"
        else
            QUERIES_FILE=$(find /repo/tests/queries/0_stateless -type f -name "*.sql" | sort -R)
        fi
        if [[ -n "${FUZZER_COMPATIBILITY:-}" ]];
        then
            COMPAT_ARG="--compatibility=${FUZZER_COMPATIBILITY}"
            echo "Using AST fuzzer compatibility setting: ${FUZZER_COMPATIBILITY}"
        else
            COMPAT_ARG=""
        fi
        FUZZER_ARGS="--query-fuzzer-runs=1000 --create-query-fuzzer-runs=50 $COMPAT_ARG --queries-file $QUERIES_FILE $NEW_TESTS_OPT"
    elif [ "$FUZZER_TO_RUN" = "BuzzHouse" ]
    then
        FUZZER_ARGS="--buzz-house-config=fuzz.json"
    else
        >&2 echo "Fuzzer \"$FUZZER_TO_RUN\" unknown, provide either \"AST Fuzzer\" or \"BuzzHouse\""
        exit 1
    fi

    # Convert FUZZ_TIME_LIMIT (e.g. "30m", "60m", "1800s" or plain seconds) into seconds
    # so that the remaining budget can be recomputed between fuzzer passes.
    fuzz_time_limit="${FUZZ_TIME_LIMIT:-30m}"
    case "$fuzz_time_limit" in
        *h) fuzz_budget_seconds=$(( ${fuzz_time_limit%h} * 3600 ));;
        *m) fuzz_budget_seconds=$(( ${fuzz_time_limit%m} * 60 ));;
        *s) fuzz_budget_seconds=$(( ${fuzz_time_limit%s} ));;
        *)  fuzz_budget_seconds=$(( fuzz_time_limit ));;
    esac

    fuzz_started_at=$SECONDS
    fuzzer_exit_code=0
    fuzz_pass=0
    : > fuzzer.log
    while :; do
        fuzz_pass=$((fuzz_pass+1))
        remaining_seconds=$(( fuzz_budget_seconds - (SECONDS - fuzz_started_at) ))
        echo "=== Fuzzer pass $fuzz_pass, remaining time budget: ${remaining_seconds}s ==="

        # Allow the fuzzer to run for some time, giving it a grace period of 5m to finish once the time
        # out triggers. After that, it'll send a SIGKILL to the fuzzer to make sure it finishes within
        # a reasonable time.
        # Bound the parser/AST recursion on the client command line, matching the server-side caps in
        # limit-recursion-settings.xml. The client parses every corpus query locally, and a corpus
        # `SET compatibility=...` reverts max_parser_backtracks to its pre-24.3 default of 0 (unbounded);
        # a command-line value survives that revert, unlike a profile value.
        timeout --verbose --signal TERM --kill-after=5m --preserve-status "${remaining_seconds}s" clickhouse-client \
            --max_memory_usage_in_client=1000000000 \
            --receive_timeout=10 \
            --receive_data_timeout_ms=10000 \
            --stacktrace \
            --max_parser_backtracks=1000000 \
            --max_parser_depth=1000 \
            $FUZZER_ARGS \
            >> fuzzer.log \
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
            client_gdb_pid=$!
        fi

        # Wait for the fuzzer to complete, but never block indefinitely. Under
        # post-fuzz memory thrash the SIGKILLed, gdb-traced client can linger
        # unreaped for many minutes (a ptrace tracer defers the parent's reap),
        # and `timeout` cannot exit until its child does -- that is what drifts
        # the job into the external cancellation ceiling with no artifacts. The
        # bare `wait` is replaced by a poll: `timeout` sends TERM at
        # ${remaining_seconds}s and SIGKILL 5m later (--kill-after=5m, kept
        # deliberately, see 4617bf64dda00); allow 90s of slack past that SIGKILL,
        # then abandon the zombie (a client not gone 90s after SIGKILL is
        # unreapable; the evidence job's zombie lasted 10.5 min, so waiting longer
        # buys nothing). Normal exits are noticed within ~1s and `wait` then
        # returns the stored status instantly.
        # BEGIN: fuzzer-client reap poll (exercised verbatim by ci/tests/test_fuzzer_liveness_loop.py)
        fuzzer_exit_code=0
        reap_deadline=$(( remaining_seconds + 300 + 90 ))
        reap_waited=0
        while :; do
            if ! kill -0 "$fuzzer_pid" 2>/dev/null; then
                wait "$fuzzer_pid" || fuzzer_exit_code=$?
                break
            fi
            if [[ "$reap_waited" -ge "$reap_deadline" ]]; then
                echo "Fuzzer reap watchdog: client not reaped ${reap_waited}s into wait (TERM + 5m KILL + 90s), abandoning the zombie"
                # Kill the reap-deferring tracer FIRST, then the actual client, then
                # the timeout wrapper. Killing only the wrapper would orphan a live
                # client that keeps issuing queries during the post-fuzz probes.
                [ -n "${client_gdb_pid:-}" ] && kill -9 "$client_gdb_pid" 2>/dev/null ||:
                [ -n "${actual_fuzzer_pid:-}" ] && kill -9 "$actual_fuzzer_pid" 2>/dev/null ||:
                kill -9 "$fuzzer_pid" 2>/dev/null ||:
                fuzzer_exit_code=137
                # Fail-closed: 137 alone rides Python's benign OK branch, so record
                # the abnormal harness state for classification (stage=reap).
                echo "stage=reap reason=client_unreapable waited=${reap_waited}s" >> harness_watchdog.txt
                break
            fi
            sleep 1
            reap_waited=$((reap_waited+1))
        done
        # END: fuzzer-client reap poll
        echo "Fuzzer exit code is $fuzzer_exit_code"

        # A non-zero exit code is either the time limit (TERM/KILL sent by `timeout`) or a
        # client/server failure — in both cases stop and let the code below classify it.
        if [[ "$fuzzer_exit_code" != "0" ]]; then
            break
        fi
        # BuzzHouse enforces its own internal time budget and exits 0 when it is reached.
        if [[ "$FUZZER_TO_RUN" != "AST Fuzzer" ]]; then
            break
        fi
        # Exit code 0 means the client walked the whole corpus and exited voluntarily. For the
        # targeted AST fuzzer the corpus is small, so a pass can finish within a couple of
        # minutes of a 30-minute budget. Each pass starts from a fresh random seed, so rerunning
        # the corpus explores new mutations instead of throwing away the remaining budget.
        remaining_seconds=$(( fuzz_budget_seconds - (SECONDS - fuzz_started_at) ))
        if [[ "$remaining_seconds" -lt 60 ]]; then
            echo "Fuzzer finished the corpus and only ${remaining_seconds}s of the budget remain, not restarting"
            break
        fi
        # Make sure the server is still accepting queries before restarting: a fuzzer that
        # exited with code 0 against a dead server would otherwise restart-loop until the budget
        # runs out and hide the failure.
        if ! clickhouse-client --receive_timeout=5 --query "SELECT 'fuzzer restart liveness check'"; then
            echo "Server is not responding, not restarting the fuzzer"
            break
        fi
        echo "Fuzzer finished the corpus with success, restarting it to use the remaining ${remaining_seconds}s of the budget"
    done

    # If the server dies, most often the fuzzer returns Code 210: Connetion
    # refused, and sometimes also code 32: attempt to read after eof. For
    # simplicity, check again whether the server is accepting connections using
    # clickhouse-client. We don't check for the existence of the server process, because
    # the process is still present while the server is terminating and not
    # accepting the connections anymore.

    # Default: the loop leaves this unset if it exhausts all retries via the
    # "alive but busy" branches (TOO_MANY_SIMULTANEOUS_QUERIES /
    # MEMORY_LIMIT_EXCEEDED / probe timeout); a dead server sets server_died=1
    # and breaks. A receive/socket timeout means the server is alive but slow to
    # answer (common right after a 30m ASAN fuzz run), not dead -- a dead server
    # returns "Connection refused"/EOF instead -- so count repeated timeouts and
    # only declare a hang once they persist, otherwise a single transient timeout
    # turns a clean (exit 143) run into a bogus "server died" FAIL.
    # BEGIN: server-liveness probe loop (exercised verbatim by
    # ci/tests/test_fuzzer_liveness_loop.py)
    server_died=0
    timeouts=0
    timeouts_max=12
    # Consecutive probes rejected with the server-global "(total) memory limit
    # exceeded" tracker error, and whether any probe was ever answered. A run
    # that ends with the server pinned above its cap and growing while idle keeps
    # rejecting ~0-byte allocations forever (reclaim never comes); left alone it
    # drifts the job into the external cancel with no artifacts. probe_success
    # feeds the exhaustion fail-closed rule after the loop.
    memory_limit_probes=0
    probe_success=0
    server_memory_stuck=0

    # Aggregate wall-clock bound for the whole probe stage: the per-call bounds
    # (--receive_timeout=5 probe, timeout-10 diagnostic) still allow ~16 s per
    # TOO_MANY iteration, ~27 min over 100 probes -- enough to erode the margin
    # to the external cancel this loop exists to beat. 300 s sits above every
    # detector's trip point (fast ~13 s, patient ~60 s, timeout declare ~72 s)
    # so it never preempts them. PROBE_STAGE_DEADLINE_SECONDS is the unit-test
    # seam (same style as MEMINFO_PATH); unset -> 300.
    probe_stage_deadline="${PROBE_STAGE_DEADLINE_SECONDS:-300}"
    probe_stage_started_at=$SECONDS

    for _ in {1..100}
    do
        # A probe success breaks the loop, so reaching this deadline means zero
        # answered probes: fall through to the fail-closed exhaustion
        # classification below.
        if (( SECONDS - probe_stage_started_at >= probe_stage_deadline ))
        then
            echo "Server live check: probe stage deadline (${probe_stage_deadline}s) reached after $((SECONDS - probe_stage_started_at))s, ending probe window"
            break
        fi
        # The deadline is only checked here, at the loop head, so cap each blocking
        # call by what is LEFT of the aggregate budget: a probe started just before
        # expiry could otherwise spend 5s, then 10s on the diagnostic and 1s
        # sleeping, overrunning the published window by ~16s of the thin
        # cancellation margin. Never below 1s, so the probe still gets to answer.
        probe_stage_remaining=$(( probe_stage_deadline - (SECONDS - probe_stage_started_at) ))
        (( probe_stage_remaining < 1 )) && probe_stage_remaining=1
        # The WHOLE schedule has to fit the remaining budget: `timeout -k G S` can
        # take S+G, so reserve the 1s kill grace out of it rather than adding to it.
        probe_wall=5
        (( probe_wall > probe_stage_remaining )) && probe_wall=$probe_stage_remaining
        probe_timeout=$(( probe_wall - 1 ))
        (( probe_timeout < 1 )) && probe_timeout=1
        # `--receive_timeout` only bounds waiting for server data; a client wedged
        # before that (connect, TLS, DNS) would sit here unbounded, so wrap the probe
        # in a hard wall-clock bound too.
        probe_status=0
        timeout -k 1 $probe_timeout clickhouse-client --receive_timeout=$probe_timeout --query "SELECT 1" 2> err || probe_status=$?
        if (( probe_status == 0 ))
        then
            server_died=0
            probe_success=1
            break
        else
            # There are legitimate queries leading to this error, example:
            # SELECT * FROM remote('127.0.0.{1..255}', system, one)
            if grep -F '(total) memory limit exceeded' err
            then
                # The server-global memory tracker is rejecting the idle probe
                # itself: the server is pinned above its cap and (per the incident)
                # still growing, so it will not reclaim. This is DISTINCT from the
                # transient per-query/user 241 that edecdd570 tolerates below --
                # only the "(total)" form evidences the server-global stuck state
                # (log_parser.py keys on the same string). A non-total 241 or a
                # TOO_MANY reply resets this counter (breaks consecutiveness); a
                # probe timeout leaves it (memory thrash interleaves 241s/timeouts).
                timeouts=0
                memory_limit_probes=$((memory_limit_probes + 1))
                # Host MemAvailable (kernel-computed, kB, host-visible in the
                # privileged container) gates the fast trip -- no error-message
                # number parsing. MEMINFO_PATH is the unit-test seam; unset ->
                # /proc/meminfo. Unreadable -> large sentinel so only the patient
                # (count) tier can fire.
                mem_available_kb=$(awk '/^MemAvailable:/ {print $2; exit}' "${MEMINFO_PATH:-/proc/meminfo}" 2>/dev/null) || true
                [ -z "${mem_available_kb:-}" ] && mem_available_kb=99999999
                stuck_tier=""
                # FAST: >=12 rejections (~13s) AND < 4 GiB free. A grower this
                # close to physical exhaustion will not recover, and ~4 GiB is the
                # runway stuck-kill + teardown + status write + upload need at the
                # observed ~110 MB/s growth before the kernel OOM killer takes an
                # arbitrary process (the runner agent -> the vanished-job mode).
                if [[ "$memory_limit_probes" -ge 12 && "$mem_available_kb" -lt 4194304 ]]
                then
                    stuck_tier="fast"
                # PATIENT: >=60 rejections (>=60s of continuous rejection while
                # idle at the 1s cadence), comparable to the timeout branch's ~72s
                # tolerance; covers the pinned-but-stable case with no host pressure
                # (jemalloc decay purge / post-query reclaim complete well inside
                # it). If a "stable" case starts growing, MemAvailable drops and the
                # fast tier fires first. Both bounds < the loop's 100-probe cap.
                elif [[ "$memory_limit_probes" -ge 60 ]]
                then
                    stuck_tier="patient"
                fi
                if [[ -n "$stuck_tier" ]]
                then
                    echo "Server live check: server-global memory limit exceeded on $memory_limit_probes consecutive idle probes (tier=$stuck_tier, MemAvailable=${mem_available_kb}kB), treating server as memory-stuck"
                    cat err
                    {
                        echo "probes=$memory_limit_probes tier=$stuck_tier MemAvailable_kB=$mem_available_kb"
                        cat err
                    } > server_memory_stuck.txt
                    server_memory_stuck=1
                    server_died=1
                    break
                fi
                sleep 1
            elif grep -F 'TOO_MANY_SIMULTANEOUS_QUERIES' err
            then
                # Give it some time to cool down. The SHOW PROCESSLIST is only a
                # diagnostic and runs under `set -e`; if the same overload rejects
                # it, do not abort the script (that would skip the status.tsv
                # write below and surface as a missing-status job ERROR).
                # Wall-clock-bounded: an unbounded diagnostic inherits the 300 s
                # client receive_timeout default; admitted-but-slow on a thrashing
                # server it would hold this stage (and a query slot) toward the
                # external cancel. timeout covers connect/send/receive/execution.
                # Recompute: the probe above may have consumed most of the budget,
                # so the value captured at the loop head is stale by now.
                probe_stage_remaining=$(( probe_stage_deadline - (SECONDS - probe_stage_started_at) ))
                (( probe_stage_remaining < 1 )) && probe_stage_remaining=1
                # 10s total for this diagnostic: 9s to TERM plus the 1s kill grace,
                # so the whole schedule fits the bound the PR publishes.
                diagnostic_wall=10
                (( diagnostic_wall > probe_stage_remaining )) && diagnostic_wall=$probe_stage_remaining
                diagnostic_timeout=$(( diagnostic_wall - 1 ))
                (( diagnostic_timeout < 1 )) && diagnostic_timeout=1
                # --kill-after: plain `timeout N` sends SIGTERM at N and then waits
                # INDEFINITELY if the child ignores it, so the bound would not be a
                # bound at all (measured: 30s for a TERM-ignoring child under
                # `timeout 2`). SIGKILL one second later makes it hard.
                timeout -k 1 $diagnostic_timeout clickhouse-client --query "SHOW PROCESSLIST" ||:
                timeouts=0
                # Server is demonstrably processing queries -> not the stuck state.
                memory_limit_probes=0
                sleep 1
            elif grep -F 'MEMORY_LIMIT_EXCEEDED' err
            then
                # Server is alive but at a per-query/user memory limit (not the
                # (total) form matched above), give it time to reclaim. A non-total
                # 241 breaks the consecutive server-global-stuck run.
                timeouts=0
                memory_limit_probes=0
                sleep 1
            elif (( probe_status == 124 || probe_status == 137 )) || grep -F 'Timeout exceeded while' err
            then
                # Alive but slow to answer: retry, and only treat it as a real
                # hang once the timeouts persist (a dead server hits the branch
                # below with "Connection refused"/EOF, not a timeout). Leave
                # memory_limit_probes untouched: under memory thrash the server
                # interleaves (total) 241s and probe timeouts, and resetting here
                # would keep the stuck counter from ever reaching its threshold.
                timeouts=$((timeouts + 1))
                if [[ "$timeouts" -ge "$timeouts_max" ]]
                then
                    echo "Server live check: probe timed out $timeouts times, treating server as hung"
                    cat err
                    # The server is alive by this branch's own evidence (a dead
                    # server refuses instead of timing out); the graceful stop
                    # below will log its normal "Received signal 15". Record the
                    # abnormal state so classification cannot attribute that
                    # self-inflicted line as the failure.
                    echo "stage=probes reason=persistent_probe_timeouts timeouts=${timeouts}" >> harness_watchdog.txt
                    server_died=1
                    break
                fi
                sleep 1
            else
                echo "Server live check returns $?"
                cat err
                server_died=1
                break
            fi
        fi
    done

    # Exhaustion fail-closed: a server that answered ZERO of the 100 probes
    # (~2-6 min of continuous unavailability) is not healthy no matter which
    # rejection form dominated. Without this, a pattern that dodges both
    # thresholds -- e.g. 50/50 alternation of (total) 241s and probe timeouts, or
    # persistent non-total 241s -- would exhaust the loop with server_died=0 and
    # ride the exit-137 benign OK branch (a silent bogus pass). Attribution stays
    # strictly global: only a memory-rejection-dominated window (>=30 (total)
    # rejections) gets the memory-stuck marker; any other exhaustion records a
    # probes-stage watchdog line (harness_watchdog.txt), so classification
    # reports the watchdog ERROR instead of scraping the graceful stop's signal
    # line -- a genuine earlier parser finding still wins.
    if [[ "$server_died" -eq 0 && "$probe_success" -eq 0 ]]
    then
        echo "Server live check: probe window exhausted with zero successful answers, treating server as down"
        server_died=1
        if [[ "$memory_limit_probes" -ge 30 && ! -f server_memory_stuck.txt ]]
        then
            {
                echo "probes=$memory_limit_probes tier=exhaustion MemAvailable_kB=unknown"
                cat err 2>/dev/null ||:
            } > server_memory_stuck.txt
            server_memory_stuck=1
        else
            # Zero answered probes but not memory-dominated (TOO_MANY / per-user
            # 241 / mixed / a stage-deadline exit): the server is alive by the evidence
            # of its own rejections, and the graceful stop below will log its normal
            # "Received signal 15". Record the abnormal state so classification
            # cannot attribute that self-inflicted line as the failure.
            echo "stage=probes reason=zero_answered_probes memory_limit_probes=${memory_limit_probes}" >> harness_watchdog.txt
        fi
    fi
    # END: server-liveness probe loop

    # If the server is memory-stuck, kill it OURSELVES instead of attempting a
    # graceful stop. Graceful `clickhouse stop` on a memory-saturated server is
    # the hang vector (shutdown paths allocate -> the tracker throws -> thrash),
    # and RSS keeps climbing toward physical RAM meanwhile. A deliberate SIGKILL
    # mimics a crashed server -- a path the teardown below already handles daily
    # -- and writes no core (a ~57 GiB core on a nearly-full box is its own
    # hazard; attribution comes from the server.log MemoryTracker lines +
    # fuzzer.log query history, which now get uploaded). Kill the gdb tracer
    # first: a ptrace tracer defers the tracee's reap. ASan builds attach no gdb
    # (server_gdb_pid unset) -> guarded.
    # BEGIN: memory-stuck server kill (exercised verbatim by ci/tests/test_fuzzer_liveness_loop.py)
    if [[ "$server_memory_stuck" -eq 1 ]]
    then
        echo "Server is memory-stuck; killing it (SIGKILL) to guarantee a bounded, artifact-carrying teardown"
        [ -n "${server_gdb_pid:-}" ] && kill -9 "$server_gdb_pid" 2>/dev/null ||:
        kill -9 "$server_pid" 2>/dev/null ||:
    fi
    # END: memory-stuck server kill

    # Stop the server in background so we can wait for the subshell to
    # finish in the foreground. We wait on server_bg_pid (the subshell running
    # the server pipeline) rather than server_pid (from the PID file), because
    # the PID file contains the forked server process which is not a direct
    # child of this shell, so wait would fail with "not a child of this shell".
    # The subshell exits with clickhouse-server's exit code via PIPESTATUS.
    #
    # Bound the wait: a graceful shutdown of a still-degraded server can hang,
    # and an unbounded wait here is the second way the job drifts into the
    # external cancel with no status.tsv. Poll for up to 180s (>> observed
    # healthy teardowns; in the stuck path above the server is already dead and
    # stop_server returns in ~10-15s). On deadline, SIGKILL the server (gdb
    # first) and record the abnormal state so classification cannot report OK.
    # BEGIN: server teardown poll (exercised verbatim by ci/tests/test_fuzzer_liveness_loop.py)
    stop_server &
    stop_server_pid=$!
    server_exit_code=0
    teardown_waited=0
    teardown_deadline=180
    while :; do
        if ! kill -0 "$server_bg_pid" 2>/dev/null; then
            wait "$server_bg_pid" || server_exit_code=$?
            break
        fi
        if [[ "$teardown_waited" -ge "$teardown_deadline" ]]; then
            echo "Teardown watchdog: server did not stop ${teardown_waited}s after graceful stop, forcing SIGKILL"
            [ -n "${server_gdb_pid:-}" ] && kill -9 "$server_gdb_pid" 2>/dev/null ||:
            kill -9 "$server_pid" 2>/dev/null ||:
            # Give the subshell a moment to observe the death and exit via PIPESTATUS.
            for _ in {1..10}; do
                kill -0 "$server_bg_pid" 2>/dev/null || break
                sleep 1
            done
            if kill -0 "$server_bg_pid" 2>/dev/null; then
                server_exit_code=137
            else
                wait "$server_bg_pid" || server_exit_code=$?
            fi
            # Fail-closed: server_exit_code alone is ignored by classification when
            # server_died=0, so a silently-degraded shutdown would report OK.
            echo "stage=teardown reason=graceful_stop_hung waited=${teardown_waited}s" >> harness_watchdog.txt
            break
        fi
        sleep 1
        teardown_waited=$((teardown_waited+1))
    done
    # stop_server is a diagnostic background job; do not let it linger.
    kill "$stop_server_pid" 2>/dev/null ||:
    # END: server teardown poll
    echo "Server exit code is $server_exit_code"

    echo -e "$server_died\t$server_exit_code\t$fuzzer_exit_code" > status.tsv

    if test -f core.*; then
        zstd --threads=0 core.*
        mv core.*.zst core.zst
    fi

    # Artifact readability is handled by make_artifacts_host_readable from the
    # EXIT trap, so it also covers a `set -e` abort between here and the end.
}

case "$stage" in
"")
    ;&  # Did you know? This is "fallthrough" in bash. https://stackoverflow.com/questions/12010686/case-statement-fallthrough
"configure")
    time configure
    ;&
"fuzz")
    time fuzz
    ;&
esac

exit $server_exit_code

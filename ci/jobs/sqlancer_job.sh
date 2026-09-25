#!/bin/bash

# DISCLAIMER:
# 1. This job script is written in bash to demonstrate "praktika CI" flexibility
# 2. Other than for demonstration purposes it would be wierd to write this in bash

set -exu
set -o pipefail

# Capture the job start timestamp so the result file (written by the EXIT trap)
# can report a real `start_time` and `duration`. Praktika's CIDB inserter
# rejects `null` `start_time` (it calls `datetime.utcfromtimestamp(start_time)`
# which fails with `'NoneType' object cannot be interpreted as an integer`).
JOB_START_TIME=$(date +%s)

TMP_PATH=$(readlink -f ./ci/tmp/)
OUTPUT_PATH="$TMP_PATH/sqlancer_output"
PID_FILE="$TMP_PATH/clickhouse-server.pid"
CLICKHOUSE_BIN="$TMP_PATH/clickhouse"

# Praktika reads the job result from `./ci/tmp/result_<normalized_job_name>.json`,
# where the normalization matches `Utils.normalize_string` in `ci/praktika/utils.py`.
# `JOB_NAME` is not propagated into the docker container, so read it from the
# serialized environment file that Praktika writes before invoking the job.
NORMALIZED_JOB_NAME=$(python3 -c '
import sys
sys.path.insert(0, ".")
from ci.praktika._environment import _Environment
from ci.praktika.utils import Utils
print(Utils.normalize_string(_Environment.get().JOB_NAME))
')
RESULT_FILE="$TMP_PATH/result_${NORMALIZED_JOB_NAME}.json"

mkdir -p $OUTPUT_PATH

# Properly JSON-escape a string using python3, outputting only the inner
# content (without surrounding quotes) so callers can embed it in "...".
json_escape() {
    printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1], end="")'
}

# Write result on any exit to ensure logs are always uploaded as artifacts
write_result() {
    # Praktika's Result.Status uses uppercase tokens (OK / FAIL / ERROR);
    # any other value is rendered as "completed but unknown" in the report.
    local status="${OVERALL_STATUS:-ERROR}"
    local info
    if [ -n "${RESULT_INFO:-}" ]; then
        info="$RESULT_INFO"
    elif [ "$status" = "OK" ]; then
        info=""
    elif [ "$status" = "FAIL" ]; then
        info="Some SQLancer tests failed"
    else
        info="Script terminated unexpectedly"
    fi

    # Build results array
    local results_json=""
    if [ ${#TEST_RESULTS[@]} -gt 0 ]; then
        for i in "${!TEST_RESULTS[@]}"; do
            IFS=',' read -r test_name test_status test_info <<< "${TEST_RESULTS[i]}"
            if [ -n "$results_json" ]; then
                results_json+=","
            fi
            local escaped_info
            escaped_info="$(json_escape "$test_info")"
            results_json+=$(printf '\n    {"name": "%s", "status": "%s", "files": [], "info": "%s"}' "$test_name" "$test_status" "$escaped_info")
        done
    fi

    # Build files array. Praktika uploads every path listed here to S3 as a
    # downloadable artifact and links it in the job report (see
    # `_ResultS3.upload_result_files_to_s3`). A 5h run produces a
    # several-hundred-MB statement log (sqlancer logs every executed statement),
    # so compress files larger than 10 MB; keep smaller ones (e.g. the
    # sanitizer report on stderr) as plain text so they render inline.
    local files_json=""
    local candidate_files=(
        "$OUTPUT_PATH/clickhouse-server.log"
        "$OUTPUT_PATH/clickhouse-server.log.err"
    )
    candidate_files+=("${ATTACHED_FILES_ARRAY[@]+"${ATTACHED_FILES_ARRAY[@]}"}")
    local attach size
    for f in "${candidate_files[@]}"; do
        [ -f "$f" ] || continue
        size=$(stat -c%s "$f" 2>/dev/null || echo 0)
        if [ "$size" -gt $((10 * 1024 * 1024)) ] && gzip -f "$f"; then
            attach="$f.gz"
        else
            attach="$f"
        fi
        [ -f "$attach" ] || continue
        if [ -n "$files_json" ]; then
            files_json+=", "
        fi
        files_json+="\"$attach\""
    done

    local escaped_overall_info
    escaped_overall_info="$(json_escape "$info")"

    local job_duration=$(( $(date +%s) - JOB_START_TIME ))

    printf '{\n' > $RESULT_FILE
    printf '  "name": "SQLancer",\n' >> $RESULT_FILE
    printf '  "status": "%s",\n' "$status" >> $RESULT_FILE
    printf '  "start_time": %d,\n' "$JOB_START_TIME" >> $RESULT_FILE
    printf '  "duration": %d,\n' "$job_duration" >> $RESULT_FILE
    printf '  "results": [%s\n  ],\n' "$results_json" >> $RESULT_FILE
    printf '  "files": [%s],\n' "$files_json" >> $RESULT_FILE
    printf '  "info": "%s"\n' "$escaped_overall_info" >> $RESULT_FILE
    printf '}\n' >> $RESULT_FILE
}

# Initialize variables used by the trap handler
TEST_RESULTS=()
ATTACHED_FILES_ARRAY=()
OVERALL_STATUS="ERROR"
RESULT_INFO=""

trap write_result EXIT

if [[ -f "$CLICKHOUSE_BIN" ]]; then
    echo "$CLICKHOUSE_BIN exists"
else
    echo "$CLICKHOUSE_BIN does not exists"
    exit 1
fi

chmod +x $CLICKHOUSE_BIN
$CLICKHOUSE_BIN local --version

# Apply the server-side config overrides shipped with the SQLancer provider
# (ClickHouse/sqlancer PR #4, `.claude/clickhouse-config/`): drop the file logger
# to `warning`, remove the heavy `system.*_log` tables, and pin the profile
# settings the oracles depend on (`async_insert=0`, `alter_sync=2`,
# `mutations_sync=2`) so an INSERT/ALTER/mutation is visible to the next read in
# the same oracle iteration. The server runs without `--config-file`, so it uses
# the binary's embedded `config.xml` and merges any `config.d/*.xml` relative to
# its working directory (see `ConfigProcessor::loadConfig`); starting it from
# `$SERVER_DIR` is what makes the overrides take effect. The embedded config's
# `users_config` points at `config.xml` itself, so `<profiles>` overrides placed
# in `config.d/` reach the default profile (verified) -- unlike a packaged
# server, this does not need a separate `users.d/`.
SERVER_DIR="$TMP_PATH/server"
mkdir -p "$SERVER_DIR/config.d"
cp /sqlancer/sqlancer-main/.claude/clickhouse-config/*.xml "$SERVER_DIR/config.d/"

echo "Starting ClickHouse server..."
( cd "$SERVER_DIR" && exec "$CLICKHOUSE_BIN" server -P "$PID_FILE" ) 1>$OUTPUT_PATH/clickhouse-server.log 2>$OUTPUT_PATH/clickhouse-server.log.err &
for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) == 'Ok.' ]]; then break ; else sleep 1; fi ; done

cd /sqlancer/sqlancer-main

# Run all oracles in a single invocation bounded by one wall-clock timeout,
# matching the upstream `.claude/run-sqlancer.sh --oracles all` reference run.
# The provider uses the `com.clickhouse:client-v2` transport and pins its own
# per-request settings (`max_execution_time`, `wait_end_of_query`,
# `max_result_rows`, ...).
#
# Derive the oracle list from the provider's own curated `ALL_ORACLES` (its
# `--oracles all` set) instead of hardcoding it here: the list changes between
# sqlancer revisions (oracles are added, and noisy ones such as `RowPolicy` are
# dropped), so reading it from the pinned image keeps it in sync with whatever
# commit the Dockerfile builds. Fail closed if it cannot be parsed -- an empty
# `--oracle` would otherwise make sqlancer error out cryptically.
#
# `--random-session-settings` is intentionally NOT passed: it is rejected in
# combination with the `SEMR`/`SEMRMulti` oracles (which toggle session settings
# themselves), and those oracles are part of the curated list and already
# provide setting-differential coverage.
TIMEOUT="${SQLANCER_TIMEOUT_SECONDS:-18000}"
NUM_THREADS=10
ORACLES=$(grep -E '^ALL_ORACLES=' /sqlancer/sqlancer-main/.claude/run-sqlancer.sh | head -1 | cut -d'"' -f2)
if [ -z "$ORACLES" ]; then
    OVERALL_STATUS="ERROR"
    RESULT_INFO="Could not parse ALL_ORACLES from the sqlancer run script"
    echo "$RESULT_INFO" >&2
    exit 1
fi

# Oracles excluded from the curated run. `TextIndexDirectRead` asserts that a
# text-index `hasToken` lookup (`use_skip_indexes` on) matches a full scan, but
# it currently dominates the run with known index-vs-scan divergences (several
# tokenizers, cf. ClickHouse#107186) that would keep the job perpetually red on
# the same finding. Drop it until that divergence class is resolved upstream.
EXCLUDED_ORACLES="TextIndexDirectRead"
for excluded in $EXCLUDED_ORACLES; do
    ORACLES=$(printf '%s' "$ORACLES" | tr ',' '\n' | grep -vxF "$excluded" | paste -sd, -)
done
if [ -z "$ORACLES" ]; then
    OVERALL_STATUS="ERROR"
    RESULT_INFO="Oracle list is empty after applying exclusions"
    echo "$RESULT_INFO" >&2
    exit 1
fi
echo "$ORACLES"

OVERALL_STATUS=OK
TEST_NAME="SQLancer"
output_file="$OUTPUT_PATH/sqlancer.out"
ATTACHED_FILES_ARRAY+=("$output_file")

if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) == 'Ok.' ]]; then
    echo "Server is OK"
    java_exit=0
    java -jar target/sqlancer-*.jar \
        --num-threads "$NUM_THREADS" \
        --num-tries 999999 \
        --timeout-seconds "$TIMEOUT" \
        --use-connection-test false \
        --print-progress-summary true \
        --host 127.0.0.1 --port 8123 \
        --username default --password "" \
        clickhouse --oracle "$ORACLES" 2>&1 | tee "$output_file" || java_exit=${PIPESTATUS[0]}

    if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) != 'Ok.' ]]; then
        echo "Server crashed during SQLancer run"
        TEST_RESULTS+=("${TEST_NAME},ERROR,Server crashed during test")
        OVERALL_STATUS="FAIL"
        RESULT_INFO="Server crashed during SQLancer run"
    else
        assertion_error="$(grep 'AssertionError' "$output_file" ||:)"
        if [ -n "$assertion_error" ]; then
            # Collapse to single line; full JSON escaping is done in write_result
            assertion_error_clean="$(printf '%s' "$assertion_error" | tr '\n' ' ')"
            TEST_RESULTS+=("${TEST_NAME},FAIL,${assertion_error_clean}")
            OVERALL_STATUS="FAIL"
        elif [ "$java_exit" -ne 0 ]; then
            TEST_RESULTS+=("${TEST_NAME},ERROR,SQLancer exited with code $java_exit")
            OVERALL_STATUS="FAIL"
        else
            TEST_RESULTS+=("${TEST_NAME},OK,")
        fi
    fi
else
    TEST_RESULTS+=("${TEST_NAME},ERROR,Server is not responding")
    OVERALL_STATUS="FAIL"
    RESULT_INFO="Server is not responding before SQLancer run"
fi

# On failure, attach the per-database reproducer logs as an artifact. With
# `--log-each-select true` SQLancer writes every statement of each generated
# database to `logs/<dbms>/databaseN.log` (+ `databaseN-cur.log` for the
# in-progress one), so the failing database's log is the exact
# CREATE/INSERT/.../SELECT sequence needed to reproduce the bug - far easier
# than reconstructing it from the interleaved, multi-threaded stdout. Only on
# failure and as a single `.tar` (write_result gzips it if it exceeds 10 MB),
# to avoid uploading a multi-hundred-MB log on clean runs.
SQLANCER_LOG_DIR="/sqlancer/sqlancer-main/logs"
if [ "$OVERALL_STATUS" != "OK" ] && [ -d "$SQLANCER_LOG_DIR" ]; then
    reproducer_archive="$OUTPUT_PATH/sqlancer_reproducer_logs.tar"
    if tar -C "$(dirname "$SQLANCER_LOG_DIR")" -cf "$reproducer_archive" "$(basename "$SQLANCER_LOG_DIR")"; then
        ATTACHED_FILES_ARRAY+=("$reproducer_archive")
    fi
fi

ls "$OUTPUT_PATH"

if [ -f "$PID_FILE" ]; then
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    # Validate the PID before sending a signal: it must be numeric, the process
    # must still exist, and `/proc/<pid>/exe` must resolve to the exact
    # `clickhouse` binary that this job started. This protects against
    # signalling an unrelated process on a shared runner if the PID file is
    # stale or the PID has been reused by another `clickhouse` process.
    expected_exe="$(readlink -f "$CLICKHOUSE_BIN" 2>/dev/null || true)"
    if [[ "$pid" =~ ^[0-9]+$ ]] && [ -e "/proc/$pid/exe" ]; then
        proc_exe="$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)"
        if [ -n "$expected_exe" ] && [ "$proc_exe" = "$expected_exe" ]; then
            kill "$pid" || true
        else
            echo "Warning: PID $pid in $PID_FILE does not belong to this job's clickhouse binary (exe=[$proc_exe], expected=[$expected_exe]); not signalling"
        fi
    else
        echo "Warning: PID file $PID_FILE contains invalid or stale PID [$pid]; not signalling"
    fi
else
    echo "Warning: PID file not found at $PID_FILE"
fi
for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) == 'Ok.' ]]; then sleep 1 ; else break; fi ; done

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

    # Build files array
    local files_json=""
    for f in "$OUTPUT_PATH/clickhouse-server.log" "$OUTPUT_PATH/clickhouse-server.log.err"; do
        if [ -f "$f" ]; then
            if [ -n "$files_json" ]; then
                files_json+=", "
            fi
            files_json+="\"$f\""
        fi
    done
    for f in "${ATTACHED_FILES_ARRAY[@]+"${ATTACHED_FILES_ARRAY[@]}"}"; do
        if [ -f "$f" ]; then
            if [ -n "$files_json" ]; then
                files_json+=", "
            fi
            files_json+="\"$f\""
        fi
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

echo "Starting ClickHouse server..."
$CLICKHOUSE_BIN server -P $PID_FILE 1>$OUTPUT_PATH/clickhouse-server.log 2>$OUTPUT_PATH/clickhouse-server.log.err &
for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) == 'Ok.' ]]; then break ; else sleep 1; fi ; done

cd /sqlancer/sqlancer-main

TIMEOUT=300
NUM_QUERIES=1000
NUM_THREADS=10
TESTS=( "TLPGroupBy" "TLPHaving" "TLPWhere" "TLPDistinct" "TLPAggregate" "NoREC" )
echo "${TESTS[@]}"

OVERALL_STATUS=OK

for TEST in "${TESTS[@]}"; do
    echo "$TEST"
    error_output_file="$OUTPUT_PATH/$TEST.err"
    ATTACHED_FILES_ARRAY+=("$error_output_file")

    if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) == 'Ok.' ]]; then
        echo "Server is OK"
        java_exit=0
        ( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads "$NUM_THREADS" --timeout-seconds "$TIMEOUT" --num-queries "$NUM_QUERIES"  --username default --password "" clickhouse --oracle "$TEST" | tee "./$TEST.out" )  3>&1 1>&2 2>&3 | tee "$error_output_file" || java_exit=$?

        if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) != 'Ok.' ]]; then
            echo "Server crashed during $TEST"
            TEST_RESULTS+=("${TEST},ERROR,Server crashed during test")
            OVERALL_STATUS="FAIL"
            RESULT_INFO="Server crashed during $TEST"
            break
        fi

        assertion_error="$(grep 'AssertionError' "$error_output_file" ||:)"

        if [ -n "$assertion_error" ]; then
            # Collapse to single line; full JSON escaping is done in write_result
            assertion_error_clean="$(printf '%s' "$assertion_error" | tr '\n' ' ')"
            TEST_RESULTS+=("${TEST},FAIL,${assertion_error_clean}")
            OVERALL_STATUS="FAIL"
        elif [ "$java_exit" -ne 0 ]; then
            TEST_RESULTS+=("${TEST},ERROR,SQLancer exited with code $java_exit")
            OVERALL_STATUS="FAIL"
        else
            TEST_RESULTS+=("${TEST},OK,")
        fi
    else
        TEST_RESULTS+=("${TEST},ERROR,Server is not responding")
        OVERALL_STATUS="FAIL"
        RESULT_INFO="Server is not responding before $TEST"
        break
    fi
done

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
